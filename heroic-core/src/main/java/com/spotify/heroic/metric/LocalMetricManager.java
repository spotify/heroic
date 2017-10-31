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
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.StreamCollector;
import java.util.ArrayList;
import java.util.HashMap;
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
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

@Slf4j
@ToString(of = {})
@MetricScope
public class LocalMetricManager implements MetricManager {
    private static final QueryTrace.Identifier QUERY =
        QueryTrace.identifier(LocalMetricManager.class, "query");
    private static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(LocalMetricManager.class, "fetch");

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
        @Named("failOnLimits") final boolean failOnLimits, final AsyncFramework async,
        final GroupSet<MetricBackend> groupSet, final MetadataManager metadata,
        final MetricBackendReporter reporter, final QueryLoggerFactory queryLoggerFactory
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

    @ToString
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
            private final MetricType source;

            private Transform(
                final FullQuery.Request request, final boolean failOnLimits,
                final OptionalLimit seriesLimit, final OptionalLimit groupLimit,
                final QuotaWatcher quotaWatcher, final DataInMemoryReporter dataInMemoryReporter
            ) {
                this.aggregation = request.getAggregation();
                this.range = request.getRange();
                this.options = request.getOptions();
                this.source = request.getSource();

                this.failOnLimits = failOnLimits;
                this.seriesLimit = seriesLimit;
                this.groupLimit = groupLimit;

                this.namedWatch = QueryTrace.watch(QUERY);
                this.quotaWatcher = quotaWatcher;

                this.dataInMemoryReporter = dataInMemoryReporter;

                final Features features = request.getFeatures();
                this.bucketStrategy = options
                    .getBucketStrategy()
                    .orElseGet(
                        () -> features.withFeature(Feature.END_BUCKET, () -> BucketStrategy.END,
                            () -> BucketStrategy.START));
            }

            @Override
            public AsyncFuture<FullQuery> transform(final FindSeries result) throws Exception {
                final ResultLimits limits;

                if (result.isLimited()) {
                    if (failOnLimits) {
                        final RequestError error = QueryError.fromMessage(
                            "The number of series requested is more than the allowed limit of " +
                                seriesLimit);

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
                    return async.resolved(FullQuery.limitsError(namedWatch.end(),
                        QueryError.fromMessage(String.format(
                            "aggregation needs to retain more data then what is allowed: %d",
                            aggregationLimit.asLong().get())),
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

                /* setup fetches */
                accept(metricBackend -> {
                    for (final Series series : result.getSeries()) {
                        fetches.add(() -> metricBackend.fetch(
                            new FetchData.Request(source, series, range, options), quotaWatcher,
                            mcr -> collector.acceptMetricsCollection(series, mcr)));
                    }
                });

                return async.eventuallyCollect(fetches, collector, fetchParallelism);
            }
        }

        @Override
        public AsyncFuture<FullQuery> query(final FullQuery.Request request) {
            if (!concurrentQueries.tryAcquire()) {
                // There's currently too many concurrent queries. Fail now so that the QueryManager
                // gets an opportunity to try another node in the same shard instead.
                return async.failed(new GoAwayException(
                    "Node has reached maximum number of concurrent MetricManager requests (" +
                        concurrentQueriesBackoff + ")"));
            }

            try {
                return protectedQuery(request).onFinished(concurrentQueries::release);
            } catch (Exception e) {
                concurrentQueries.release();
                throw new RuntimeException(e);
            }
        }

        private AsyncFuture<FullQuery> protectedQuery(final FullQuery.Request request) {
            final QueryOptions options = request.getOptions();
            final QueryContext queryContext = request.getContext();

            queryLogger.logIncomingRequestAtNode(queryContext, request);

            final DataInMemoryReporter dataInMemoryReporter = reporter.newDataInMemoryReporter();

            final QuotaWatcher quotaWatcher = new QuotaWatcher(
                options.getDataLimit().orElse(dataLimit).asLong().orElse(Long.MAX_VALUE), options
                .getAggregationLimit()
                .orElse(aggregationLimit)
                .asLong()
                .orElse(Long.MAX_VALUE), dataInMemoryReporter);

            final OptionalLimit seriesLimit =
                options.getSeriesLimit().orElse(LocalMetricManager.this.seriesLimit);

            final boolean failOnLimits =
                options.getFailOnLimits().orElse(LocalMetricManager.this.failOnLimits);

            final OptionalLimit groupLimit =
                options.getGroupLimit().orElse(LocalMetricManager.this.groupLimit);

            // Transform that takes the result from ES metadata lookup to fetch from backend
            final LazyTransform<FindSeries, FullQuery> transform =
                new Transform(request, failOnLimits, seriesLimit, groupLimit, quotaWatcher,
                    dataInMemoryReporter);

            return metadata
                .findSeries(
                    new FindSeries.Request(request.getFilter(), request.getRange(), seriesLimit))
                .onDone(reporter.reportFindSeries())
                .lazyTransform(transform)
                .directTransform(fullQuery -> {
                    queryLogger.logOutgoingResponseAtNode(queryContext, fullQuery);
                    return fullQuery;
                })
                .onDone(reporter.reportQueryMetrics());
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
            final FetchData.Request request, final FetchQuotaWatcher watcher,
            final Consumer<MetricReadResult> metricsConsumer
        ) {
            final List<AsyncFuture<FetchData.Result>> callbacks =
                map(b -> b.fetch(request, watcher, metricsConsumer));
            return async.collect(callbacks, FetchData.collectResult(FETCH));
        }

        @Override
        public AsyncFuture<WriteMetric> write(final WriteMetric.Request write) {
            return async.collect(map(b -> b.write(write)), WriteMetric.reduce());
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
                    collections.add(result.getData());
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

    @RequiredArgsConstructor
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

        @Override
        public void resolved(final FetchData.Result result) throws Exception {
            requestErrors.addAll(result.getErrors());
        }

        void acceptMetricsCollection(
            final Series series, final MetricReadResult readResult
        ) {
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
                errorsBuilder.add(QueryError.fromMessage(
                    checkIssues(failed, cancelled).orElse("Query exceeded quota")));

                return new FullQuery(trace, errorsBuilder.build(), ImmutableList.of(),
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
                Statistics.of(ROWS_ACCESSED, watcher.getRowsAccessed());

            final List<ResultGroup> groups = new ArrayList<>();

            for (final AggregationOutput group : result.getResult()) {
                if (groupLimit.isGreaterOrEqual(groups.size())) {
                    if (failOnLimits) {
                        errorsBuilder.add(QueryError.fromMessage(
                            "The number of result groups is more than the allowed limit of " +
                                groupLimit));
                        return new FullQuery(trace, errorsBuilder.build(), ImmutableList.of(),
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

            return new FullQuery(trace, errorsBuilder.build(), groups,
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
            final Histogram.Builder builder = Histogram.builder();
            for (final Long value : rowDensityData.elementSet()) {
                builder.add(value);
            }
            return builder.build();
        }
    }

    @RequiredArgsConstructor
    private static class QuotaWatcher implements FetchQuotaWatcher, RetainQuotaWatcher {
        private final long dataLimit;
        private final long retainLimit;
        private final DataInMemoryReporter dataInMemoryReporter;

        private final AtomicLong read = new AtomicLong();
        private final AtomicLong retained = new AtomicLong();

        private final LongAdder rowsAccessed = new LongAdder();

        @Override
        public void readData(long n) {
            read.addAndGet(n);
            throwIfViolated();
            // Must be called after checkViolation above, since that one might throw an exception.
            dataInMemoryReporter.reportDataHasBeenRead(n);
        }

        @Override
        public void retainData(final long n) {
            retained.addAndGet(n);
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

        private static int getLeft(long limit, long current) {
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
