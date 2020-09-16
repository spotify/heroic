/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.metric;

import static io.opencensus.trace.AttributeValue.booleanAttributeValue;
import static io.opencensus.trace.AttributeValue.longAttributeValue;
import static java.lang.String.format;

import com.google.common.collect.ConcurrentHashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationOutput;
import com.spotify.heroic.aggregation.AggregationResult;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.BucketStrategy;
import com.spotify.heroic.aggregation.RetainQuotaWatcher;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.GoAwayException;
import com.spotify.heroic.common.GroupSet;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Histogram;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.QuotaViolationException;
import com.spotify.heroic.common.SelectedGroup;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.querylogging.QueryContext;
import com.spotify.heroic.querylogging.QueryLogger;
import com.spotify.heroic.querylogging.QueryLoggerFactory;
import com.spotify.heroic.statistics.DataInMemoryReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.tracing.EndSpanFutureReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.StreamCollector;
import eu.toolchain.async.FutureDone;
import io.opencensus.trace.Span;
import io.opencensus.trace.Tracer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.inject.Inject;
import javax.inject.Named;
import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;

@MetricScope
public class LocalMetricManager implements MetricManager {
    private static final Tracer tracer = io.opencensus.trace.Tracing.getTracer();
    private static final QueryTrace.Identifier QUERY =
        QueryTrace.identifier(LocalMetricManager.class, "query");
    private static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(LocalMetricManager.class, "fetch");
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(LocalMetricManager.class);

    private final OptionalLimit groupLimit;
    private final OptionalLimit seriesLimit;
    private final OptionalLimit aggregationLimit;
    private final OptionalLimit dataLimit;
    private final int concurrentQueriesBackoff;
    private final int fetchParallelism;
    private final boolean failOnLimits;

    private final AsyncFramework async;
    private final GroupSet<MetricBackend> groupSet;
    private final MetadataManager metadata;
    private final MetricBackendReporter reporter;
    private final QueryLogger queryLogger;
    private final Semaphore concurrentQueries;
    private static final HashSet<QuotaWatcher> quotaWatchers = new HashSet<>();

    /**
     * @param groupLimit The maximum amount of groups this manager will allow to be generated.
     * @param seriesLimit The maximum amount of series in total an entire query may use.
     * @param aggregationLimit The maximum number of (estimated) data points a single aggregation
     * may produce.
     * @param dataLimit The maximum number of samples a single query is allowed to fetch.
     * @param fetchParallelism How many fetches that are allowed to be performed in parallel.
     */
    @Inject
    public LocalMetricManager(
        @Named("groupLimit") final OptionalLimit groupLimit,
        @Named("seriesLimit") final OptionalLimit seriesLimit,
        @Named("aggregationLimit") final OptionalLimit aggregationLimit,
        @Named("dataLimit") final OptionalLimit dataLimit,
        @Named("concurrentQueriesBackoff") final OptionalLimit concurrentQueriesBackoff,
        @Named("fetchParallelism") final int fetchParallelism,
        @Named("failOnLimits") final boolean failOnLimits,
        final AsyncFramework async,
        final GroupSet<MetricBackend> groupSet,
        final MetadataManager metadata,
        final MetricBackendReporter reporter,
        final QueryLoggerFactory queryLoggerFactory
    ) {
        this.groupLimit = groupLimit;
        this.seriesLimit = seriesLimit;
        this.aggregationLimit = aggregationLimit;
        this.dataLimit = dataLimit;
        this.concurrentQueriesBackoff = concurrentQueriesBackoff.asMaxInteger(Integer.MAX_VALUE);
        this.fetchParallelism = fetchParallelism;
        this.failOnLimits = failOnLimits;
        this.async = async;
        this.groupSet = groupSet;
        this.metadata = metadata;
        this.reporter = reporter;
        this.queryLogger = queryLoggerFactory.create("LocalMetricManager");
        this.concurrentQueries = new Semaphore(this.concurrentQueriesBackoff);
    }

    @Override
    public GroupSet<MetricBackend> groupSet() {
        return groupSet;
    }

    @Override
    public MetricBackendGroup useOptionalGroup(final Optional<String> group) {
        return new Group(groupSet.useOptionalGroup(group), metadata.useDefaultGroup());
    }

    public String toString() {
        return "LocalMetricManager()";
    }

    private class Group extends AbstractMetricBackend implements MetricBackendGroup {
        private final SelectedGroup<MetricBackend> backends;
        private final MetadataBackend metadata;

        public Group(final SelectedGroup<MetricBackend> backends, final MetadataBackend metadata) {
            super(async);
            this.backends = backends;
            this.metadata = metadata;
        }

        @Override
        public Groups groups() {
            return backends.groups();
        }

        @Override
        public boolean isEmpty() {
            return backends.isEmpty();
        }

        public String toString() {
            return "LocalMetricManager.Group(backends=" + this.backends + ", metadata="
                   + this.metadata
                   + ")";
        }

        private class Transform implements LazyTransform<FindSeries, FullQuery> {

            private final AggregationInstance aggregation;
            private final boolean failOnLimits;
            private final OptionalLimit seriesLimit;
            private final OptionalLimit groupLimit;
            private final QueryTrace.NamedWatch namedWatch;
            private final QuotaWatcher quotaWatcher;
            private final BucketStrategy bucketStrategy;
            private final DateRange range;
            private final QueryOptions options;
            private final DataInMemoryReporter dataInMemoryReporter;
            private final Span parentSpan;
            private final MetricType source;

            private Transform(
                final FullQuery.Request request,
                final boolean failOnLimits,
                final OptionalLimit seriesLimit,
                final OptionalLimit groupLimit,
                final QuotaWatcher quotaWatcher,
                final DataInMemoryReporter dataInMemoryReporter,
                final Span parentSpan
            ) {
                this.aggregation = request.aggregation();
                this.range = request.range();
                this.options = request.options();
                this.source = request.source();

                this.failOnLimits = failOnLimits;
                this.seriesLimit = seriesLimit;
                this.groupLimit = groupLimit;

                this.namedWatch = QueryTrace.watch(QUERY);
                this.quotaWatcher = quotaWatcher;

                this.dataInMemoryReporter = dataInMemoryReporter;
                this.parentSpan = parentSpan;

                final Features features = request.features();
                this.bucketStrategy = options
                    .bucketStrategy()
                    .orElseGet(
                        () -> features.withFeature(Feature.END_BUCKET, () -> BucketStrategy.END,
                            () -> BucketStrategy.START));
            }

            @Override
            public AsyncFuture<FullQuery> transform(final FindSeries result) throws Exception {
                final Span fetchSpan = tracer.spanBuilderWithExplicitParent(
                    "localMetricsManager.fetch", parentSpan).startSpan();
                final ResultLimits limits;

                if (result.getLimited()) {
                    if (failOnLimits) {
                        final RequestError error = new QueryError(
                            "The number of series requested is more than the allowed limit of " +
                                seriesLimit);

                        fetchSpan.addAnnotation(error.toString());
                        fetchSpan.putAttribute("quotaViolation", booleanAttributeValue(true));
                        fetchSpan.end();

                        return async.resolved(FullQuery.limitsError(namedWatch.end(), error,
                            ResultLimits.of(ResultLimit.SERIES)));
                    }

                    limits = ResultLimits.of(ResultLimit.SERIES);
                } else {
                    limits = ResultLimits.of();
                }

                /* if empty, there are not time series on this shard */
                if (result.isEmpty()) {
                    return async.resolved(FullQuery.empty(namedWatch.end(), limits));
                }

                final AggregationSession session;
                try {
                    session = aggregation.session(range, quotaWatcher, bucketStrategy);
                } catch (QuotaViolationException e) {
                    String error = format(
                        "aggregation needs to retain more data then what is allowed: %d",
                        aggregationLimit.asLong().get());
                    fetchSpan.addAnnotation(error);
                    fetchSpan.putAttribute("quotaViolation", booleanAttributeValue(true));
                    fetchSpan.end();
                    return async.resolved(FullQuery.limitsError(namedWatch.end(),
                        new QueryError(error),
                        ResultLimits.of(ResultLimit.AGGREGATION)));
                }

                /* setup collector */
                final ResultCollector collector;

                if (options.tracing().isEnabled(Tracing.DETAILED)) {
                    // tracing enabled, keeps track of each individual FetchData trace.
                    collector = new ResultCollector(quotaWatcher, dataInMemoryReporter, aggregation,
                        session, limits, groupLimit, failOnLimits) {
                        final ConcurrentLinkedQueue<QueryTrace> traces =
                            new ConcurrentLinkedQueue<>();

                        @Override
                        public void resolved(final FetchData.Result result) throws Exception {
                            traces.add(result.getTrace());
                            super.resolved(result);
                        }

                        @Override
                        public QueryTrace buildTrace() {
                            return namedWatch.end(ImmutableList.copyOf(traces));
                        }
                    };
                } else {
                    // very limited tracing, does not collected each individual FetchData trace.
                    collector = new ResultCollector(quotaWatcher, dataInMemoryReporter, aggregation,
                        session, limits, groupLimit, failOnLimits) {
                        @Override
                        public QueryTrace buildTrace() {
                            return namedWatch.end();
                        }
                    };
                }

                final List<Callable<AsyncFuture<FetchData.Result>>> fetches = new ArrayList<>();
                accept(metricBackend -> {
                    for (final Series series : result.getSeries()) {
                        // Requires the squashing exporter otherwise too many spans are produced.
                        final Span fetchSeries =
                            tracer.spanBuilderWithExplicitParent(
                                "localMetricsManager.fetchSeries", fetchSpan).startSpan();

                        fetchSeries.addAnnotation(series.toString());
                        fetches.add(() -> metricBackend.fetch(
                            new FetchData.Request(source, series, range, options),
                            quotaWatcher,
                            mcr -> collector.acceptMetricsCollection(series, mcr),
                            fetchSeries
                        ).onDone(new EndSpanFutureReporter(fetchSeries)));
                    }
                });
                return async
                    .eventuallyCollect(fetches, collector, fetchParallelism)
                    .onDone(new EndSpanFutureReporter(fetchSpan));
            }
        }

        @Override
        public AsyncFuture<FullQuery> query(final FullQuery.Request request) {
            return query(request, tracer.getCurrentSpan());
        }

        @Override
        public AsyncFuture<FullQuery> query(
            final FullQuery.Request request, final Span parentSpan) {
            if (!concurrentQueries.tryAcquire()) {
                // There's currently too many concurrent queries. Fail now so that the QueryManager
                // gets an opportunity to try another node in the same shard instead.
                final String e =
                    "Node has reached maximum number of concurrent MetricManager requests (" +
                    concurrentQueriesBackoff + ")";
                parentSpan.addAnnotation(e);
                parentSpan.end();
                return async.failed(new GoAwayException(e));
            }

            try {
                return protectedQuery(request, parentSpan).onFinished(concurrentQueries::release);
            } catch (Exception e) {
                concurrentQueries.release();
                throw new RuntimeException(e);
            }
        }

        private AsyncFuture<FullQuery> protectedQuery(
            final FullQuery.Request request, final Span parentSpan) {
            final QueryOptions options = request.options();
            final QueryContext queryContext = request.context();

            queryLogger.logIncomingRequestAtNode(queryContext, request);

            final DataInMemoryReporter dataInMemoryReporter = reporter.newDataInMemoryReporter();

            final QuotaWatcher quotaWatcher = new QuotaWatcher(
                options.dataLimit().orElse(dataLimit).asLong().orElse(Long.MAX_VALUE),
                options.aggregationLimit().orElse(aggregationLimit).asLong().orElse(Long.MAX_VALUE),
                dataInMemoryReporter
            );

            // Add watcher to set to compute stats across all queries
            quotaWatchers.add(quotaWatcher);

            final OptionalLimit seriesLimit =
                options.seriesLimit().orElse(LocalMetricManager.this.seriesLimit);

            final boolean failOnLimits =
                options.failOnLimits().orElse(LocalMetricManager.this.failOnLimits);

            final OptionalLimit groupLimit =
                options.groupLimit().orElse(LocalMetricManager.this.groupLimit);

            final Span findSeriesSpan = tracer.spanBuilderWithExplicitParent(
                "localMetricsManager.findSeries", parentSpan).startSpan();

            // Transform that takes the result from ES metadata lookup to fetch from backend
            final LazyTransform<FindSeries, FullQuery> transform =
                new Transform(request,
                    failOnLimits,
                    seriesLimit,
                    groupLimit,
                    quotaWatcher,
                    dataInMemoryReporter,
                    findSeriesSpan);

            return metadata
                .findSeries(FindSeries.Request.withLimit(request, seriesLimit))
                .onDone(reporter.reportFindSeries())
                .onResolved(t -> findSeriesSpan.putAttribute(
                    "seriesCount", longAttributeValue(t.getSeries().size())))
                .lazyTransform(transform)
                .directTransform(fullQuery -> {
                    queryLogger.logOutgoingResponseAtNode(queryContext, fullQuery);
                    return fullQuery;
                })
                .onDone(reporter.reportQueryMetrics())
                .onDone(new EndSpanFutureReporter(findSeriesSpan))
                .onDone(new FutureDone<>() {
                    @Override
                    public void failed(final Throwable cause) throws Exception {
                        cleanQuotaWatchers(quotaWatcher);
                        log.info("onDone.failed {}", quotaWatchers.size());
                    }

                    @Override
                    public void resolved(final FullQuery result) throws Exception {
                        cleanQuotaWatchers(quotaWatcher);
                        log.info("onDone.resolved {}", quotaWatchers.size());
                    }

                    @Override
                    public void cancelled() throws Exception {
                        cleanQuotaWatchers(quotaWatcher);
                        log.info("onDone.cancelled {}", quotaWatchers.size());
                    }
                });
        }

        private void cleanQuotaWatchers(QuotaWatcher quotaWatcher) {
            // Let's find any old watchers based on start timestamp
            quotaWatchers.remove(quotaWatcher);
            log.info("Millis QuotaWatcher was alive: {}",
                (System.currentTimeMillis() - quotaWatcher.startMS));

            quotaWatchers.removeIf(qw -> {
                if (System.currentTimeMillis() - qw.startMS > 120_000) {
                    log.info("Removing QuotaWatcher with MS alive: {}",
                        (System.currentTimeMillis() - qw.startMS));
                    return true;
                }
                return false;
            });
        }

        @Override
        public Statistics getStatistics() {
            Statistics result = Statistics.empty();

            for (final Statistics s : map(MetricBackend::getStatistics)) {
                result = result.merge(s);
            }

            return result;
        }

        @Override
        public AsyncFuture<FetchData.Result> fetch(
            final FetchData.Request request,
            final FetchQuotaWatcher watcher,
            final Consumer<MetricReadResult> metricsConsumer,
            final Span parentSpan
        ) {
            final List<AsyncFuture<FetchData.Result>> callbacks =
                map(b -> b.fetch(request, watcher, metricsConsumer, parentSpan));
            return async.collect(callbacks, FetchData.collectResult(FETCH));
        }

        @Override
        public AsyncFuture<WriteMetric> write(final WriteMetric.Request request) {
            return write(request, io.opencensus.trace.Tracing.getTracer().getCurrentSpan());
        }

        @Override
        public AsyncFuture<WriteMetric> write(
            final WriteMetric.Request request, final Span parentSpan
        ) {
            return async.collect(map(b -> b.write(request, parentSpan)), WriteMetric.reduce());
        }

        @Override
        public AsyncObservable<BackendKeySet> streamKeys(
            final BackendKeyFilter filter, final QueryOptions options
        ) {
            return AsyncObservable.chain(map(b -> b.streamKeys(filter, options)));
        }

        @Override
        public boolean isReady() {
            for (final MetricBackend backend : backends) {
                if (!backend.isReady()) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public Iterable<BackendEntry> listEntries() {
            throw new NotImplementedException("not supported");
        }

        @Override
        public AsyncFuture<Void> configure() {
            return async.collectAndDiscard(map(MetricBackend::configure));
        }

        @Override
        public AsyncFuture<List<String>> serializeKeyToHex(BackendKey key) {
            return async
                .collect(map(b -> b.serializeKeyToHex(key)))
                .directTransform(result -> ImmutableList.copyOf(Iterables.concat(result)));
        }

        @Override
        public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
            return async
                .collect(map(b -> b.deserializeKeyFromHex(key)))
                .directTransform(result -> ImmutableList.copyOf(Iterables.concat(result)));
        }

        @Override
        public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
            return async.collectAndDiscard(map(b -> b.deleteKey(key, options)));
        }

        @Override
        public AsyncFuture<Long> countKey(BackendKey key, QueryOptions options) {
            return async.collect(map(b -> b.countKey(key, options))).directTransform(result -> {
                long count = 0;

                for (final long c : result) {
                    count += c;
                }

                return count;
            });
        }

        @Override
        public AsyncFuture<MetricCollection> fetchRow(final BackendKey key) {
            final List<AsyncFuture<MetricCollection>> callbacks = map(b -> b.fetchRow(key));

            return async.collect(callbacks, results -> {
                final List<List<? extends Metric>> collections = new ArrayList<>();

                for (final MetricCollection result : results) {
                    collections.add(result.data());
                }

                return MetricCollection.mergeSorted(key.getType(), collections);
            });
        }

        @Override
        public AsyncObservable<MetricCollection> streamRow(final BackendKey key) {
            return AsyncObservable.chain(map(b -> b.streamRow(key)));
        }

        private void accept(final Consumer<MetricBackend> op) {
            backends.stream().forEach(op::accept);
        }

        private <T> List<T> map(final Function<MetricBackend, T> op) {
            return ImmutableList.copyOf(backends.stream().map(op).iterator());
        }
    }

    private abstract static class ResultCollector
        implements StreamCollector<FetchData.Result, FullQuery> {
        private static final String ROWS_ACCESSED = "rowsAccessed";

        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();
        final ConcurrentLinkedQueue<RequestError> requestErrors = new ConcurrentLinkedQueue<>();

        final QuotaWatcher watcher;
        final DataInMemoryReporter dataInMemoryReporter;
        final AggregationInstance aggregation;
        final AggregationSession session;
        final ResultLimits limits;
        final OptionalLimit groupLimit;
        final boolean failOnLimits;

        private final ConcurrentHashMultiset<Long> rowDensityData = ConcurrentHashMultiset.create();

        private ResultCollector(
            final QuotaWatcher watcher,
            final DataInMemoryReporter dataInMemoryReporter,
            final AggregationInstance aggregation,
            final AggregationSession session,
            final ResultLimits limits,
            final OptionalLimit groupLimit,
            final boolean failOnLimits
        ) {
            this.watcher = watcher;
            this.dataInMemoryReporter = dataInMemoryReporter;
            this.aggregation = aggregation;
            this.session = session;
            this.limits = limits;
            this.groupLimit = groupLimit;
            this.failOnLimits = failOnLimits;
        }

        @Override
        public void resolved(final FetchData.Result result) throws Exception {
            requestErrors.addAll(result.getErrors());
        }

        void acceptMetricsCollection(final Series series, final MetricReadResult readResult) {
            final MetricCollection metrics = readResult.getMetrics();
            final Map<String, String> aggregationKey = buildAggregationKey(series, readResult);

            metrics.updateAggregation(session, aggregationKey,
                ImmutableSet.of(series.withResource(readResult.getResource())));
            dataInMemoryReporter.reportDataNoLongerNeeded(metrics.size());

            metrics.getAverageDistanceBetweenMetrics().ifPresent(msBetweenSamples -> {
                final double metricsPerSecond = 1000.0 / msBetweenSamples;
                dataInMemoryReporter.reportRowDensity(metricsPerSecond);
                final long metricsPerMegaSecond = (long) (metricsPerSecond * 1_000_000);
                rowDensityData.add(metricsPerMegaSecond);
            });
        }

        private Map<String, String> buildAggregationKey(
            final Series series, final MetricReadResult readResult
        ) {
            if (readResult.getResource().isEmpty()) {
                return series.getTags();
            }

            final Map<String, String> key = new HashMap<>(series.getTags());
            key.putAll(readResult.getResource());
            return key;
        }

        @Override
        public void failed(final Throwable cause) throws Exception {
            errors.add(cause);
        }

        @Override
        public void cancelled() throws Exception {
        }

        public abstract QueryTrace buildTrace();

        @Override
        public FullQuery end(int resolved, int failed, int cancelled) throws Exception {
            final QueryTrace trace = buildTrace();
            final ImmutableList.Builder<RequestError> errorsBuilder = ImmutableList.builder();
            errorsBuilder.addAll(requestErrors);

            // Signal that we're done processing this
            dataInMemoryReporter.reportOperationEnded();

            final ImmutableSet.Builder<ResultLimit> limitsBuilder =
                ImmutableSet.<ResultLimit>builder().addAll(this.limits.getLimits());

            if (watcher.isRetainQuotaViolated()) {
                limitsBuilder.add(ResultLimit.AGGREGATION);
            }

            if (watcher.isReadQuotaViolated()) {
                limitsBuilder.add(ResultLimit.QUOTA);
            }

            if (watcher.isReadQuotaViolated() || watcher.isRetainQuotaViolated()) {
                final Optional<Histogram> dataDensity = Optional.of(getRowDensityHistogram());
                errorsBuilder.add(new QueryError(
                    checkIssues(failed, cancelled).orElse("Query exceeded quota")));

                return FullQuery.create(trace, errorsBuilder.build(), ImmutableList.of(),
                    Statistics.empty(), new ResultLimits(limitsBuilder.build()), dataDensity);
            }

            checkIssues(failed, cancelled).map(RuntimeException::new).ifPresent(e -> {
                for (final Throwable t : errors) {
                    e.addSuppressed(t);
                }

                throw e;
            });

            final AggregationResult result = session.result();

            final Optional<Histogram> dataDensity = Optional.of(getRowDensityHistogram());

            final Statistics baseStatistics =
                new Statistics(ROWS_ACCESSED, watcher.getRowsAccessed());

            final List<ResultGroup> groups = new ArrayList<>();

            for (final AggregationOutput group : result.getResult()) {
                if (groupLimit.isGreaterOrEqual(groups.size())) {
                    if (failOnLimits) {
                        errorsBuilder.add(new QueryError(
                            "The number of result groups is more than the allowed limit of " +
                                groupLimit));
                        return FullQuery.create(trace, errorsBuilder.build(), ImmutableList.of(),
                            baseStatistics,
                            new ResultLimits(limitsBuilder.add(ResultLimit.GROUP).build()),
                            dataDensity);
                    }

                    limitsBuilder.add(ResultLimit.GROUP);
                    break;
                }

                groups.add(new ResultGroup(group.getKey(), group.getSeries(), group.getMetrics(),
                    aggregation.cadence()));
            }

            return FullQuery.create(trace, errorsBuilder.build(), groups,
                baseStatistics.merge(result.getStatistics()),
                new ResultLimits(limitsBuilder.build()), dataDensity);
        }

        private Optional<String> checkIssues(final int failed, final int cancelled) {
            if (failed > 0 || cancelled > 0) {
                return Optional.of(
                    "Some fetches failed (" + failed + ") or were cancelled (" + cancelled + ")");
            }

            return Optional.empty();
        }

        public Histogram getRowDensityHistogram() {
            /* The data is gathered in an efficient ConcurrentHashMultiset, to allow for multiple
             * threads writing with minimum blocking. Reading the data only happens at the end of
             * the watched operation, so here we build the histogram.
             */
            final Histogram.Builder builder = new Histogram.Builder();
            for (final Long value : rowDensityData.elementSet()) {
                builder.add(value);
            }
            return builder.build();
        }
    }

    private class QuotaWatcher implements FetchQuotaWatcher, RetainQuotaWatcher {
        private final long dataLimit;
        private final long retainLimit;
        private final DataInMemoryReporter dataInMemoryReporter;
        private static final long LOGLIMIT = 1_000_000;

        private final AtomicLong read = new AtomicLong();
        private final AtomicLong retained = new AtomicLong();
        private final long startMS;
        private final LongAdder rowsAccessed = new LongAdder();

        private QuotaWatcher(final long dataLimit, final long retainLimit,
                             final DataInMemoryReporter dataInMemoryReporter) {
            this.dataLimit = dataLimit;
            this.retainLimit = retainLimit;
            this.dataInMemoryReporter = dataInMemoryReporter;
            this.startMS = System.currentTimeMillis();
        }

        @Override
        public void readData(long n) {
            long curDataPoints = read.addAndGet(n);
            long total = quotaWatchers.stream().map(QuotaWatcher::getReadData)
                .reduce(0L, Long::sum);
            reporter.reportGlobalReadDataPoints(total);
            if (curDataPoints > LOGLIMIT) {
                log.info("Data Points READ: Instance {}; This Query: {}; Delta: {}" +
                    " (# Watchers: {})", total, curDataPoints,
                    n, quotaWatchers.size());
            }
            throwIfViolated();
            // Must be called after checkViolation above, since that one might throw an exception.
            dataInMemoryReporter.reportDataHasBeenRead(n);
        }

        @Override
        public void retainData(final long n) {
            long curRetainedDataPoints = retained.addAndGet(n);
            long total = quotaWatchers.stream().map(QuotaWatcher::getRetainData)
                .reduce(0L, Long::sum);
            reporter.reportGlobalRetainedDataPoints(total);
            if (curRetainedDataPoints > LOGLIMIT) {
                log.info("Data Points RETAINED: Instance {}; This Query: {}; Delta: {} " +
                    "(# Watchers: {})", total, curRetainedDataPoints,
                    n, quotaWatchers.size());
            }
            throwIfViolated();
        }

        @Override
        public boolean mayReadData() {
            return !isReadQuotaViolated() && !isRetainQuotaViolated();
        }

        @Override
        public boolean mayRetainMoreData() {
            return mayReadData();
        }

        @Override
        public int getReadDataQuota() {
            return getLeft(dataLimit, read.get());
        }

        @Override
        public void accessedRows(final long n) {
            dataInMemoryReporter.reportRowsAccessed(n);
            rowsAccessed.add(n);
        }

        public long getReadData() {
            return read.get();
        }

        public long getRetainData() {
            return retained.get();
        }

        public long getRowsAccessed() {
            return rowsAccessed.longValue();
        }

        @Override
        public int getRetainQuota() {
            return getLeft(retainLimit, retained.get());
        }

        private void throwIfViolated() {
            if (isReadQuotaViolated() || isRetainQuotaViolated()) {
                throw new QuotaViolationException();
            }
        }

        boolean isReadQuotaViolated() {
            return read.get() >= dataLimit;
        }

        boolean isRetainQuotaViolated() {
            return retained.get() >= retainLimit;
        }

        private int getLeft(long limit, long current) {
            final long left = limit - current;

            if (left < 0) {
                return 0;
            }

            if (left > Integer.MAX_VALUE) {
                throw new IllegalStateException("quota too large");
            }

            return (int) left;
        }
    }
}
