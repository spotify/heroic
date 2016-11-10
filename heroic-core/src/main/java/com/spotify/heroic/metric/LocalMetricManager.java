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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationOutput;
import com.spotify.heroic.aggregation.AggregationResult;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.GroupSet;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.SelectedGroup;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.statistics.MetricBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.StreamCollector;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Slf4j
@ToString(of = {})
@MetricScope
public class LocalMetricManager implements MetricManager {
    private static final QueryTrace.Identifier QUERY =
        QueryTrace.identifier(LocalMetricManager.class, "query");
    private static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(LocalMetricManager.class, "fetch");

    private static Logger queryLog = LoggerFactory.getLogger("query.log");

    private final OptionalLimit groupLimit;
    private final OptionalLimit seriesLimit;
    private final OptionalLimit aggregationLimit;
    private final OptionalLimit dataLimit;
    private final int fetchParallelism;
    private final boolean failOnLimits;

    private final AsyncFramework async;
    private final GroupSet<MetricBackend> groupSet;
    private final MetadataManager metadata;
    private final MetricBackendReporter reporter;

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
        @Named("fetchParallelism") final int fetchParallelism,
        @Named("failOnLimits") final boolean failOnLimits, final AsyncFramework async,
        final GroupSet<MetricBackend> groupSet, final MetadataManager metadata,
        final MetricBackendReporter reporter
    ) {
        this.groupLimit = groupLimit;
        this.seriesLimit = seriesLimit;
        this.aggregationLimit = aggregationLimit;
        this.dataLimit = dataLimit;
        this.fetchParallelism = fetchParallelism;
        this.failOnLimits = failOnLimits;
        this.async = async;
        this.groupSet = groupSet;
        this.metadata = metadata;
        this.reporter = reporter;
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

        @Override
        public AsyncFuture<FullQuery> query(final FullQuery.Request request) {
            final QueryTrace.NamedWatch w = QueryTrace.watch(QUERY);

            final Filter filter = request.getFilter();
            final MetricType source = request.getSource();
            final QueryOptions options = request.getOptions();
            final AggregationInstance aggregation = request.getAggregation();
            final DateRange range = request.getRange();

            final FetchQuotaWatcher watcher =
                options.getDataLimit().orElse(dataLimit).asLong().<FetchQuotaWatcher>map(
                    LimitedFetchQuotaWatcher::new).orElse(FetchQuotaWatcher.NO_QUOTA);

            final OptionalLimit seriesLimit =
                options.getSeriesLimit().orElse(LocalMetricManager.this.seriesLimit);

            final boolean failOnLimits =
                options.getFailOnLimits().orElse(LocalMetricManager.this.failOnLimits);

            final LazyTransform<FindSeries, FullQuery> transform = (final FindSeries result) -> {
                final ResultLimits limits;

                if (result.isLimited()) {
                    if (failOnLimits) {
                        final List<RequestError> errors = ImmutableList.of(QueryError.fromMessage(
                            "The number of series requested is more than the allowed limit of " +
                                seriesLimit));

                        return async.resolved(
                            new FullQuery(w.end(), errors, ImmutableList.of(), Statistics.empty(),
                                ResultLimits.of(ResultLimit.SERIES)));
                    }

                    limits = ResultLimits.of(ResultLimit.SERIES);
                } else {
                    limits = ResultLimits.of();
                }

                /* if empty, there are not time series on this shard */
                if (result.isEmpty()) {
                    return async.resolved(FullQuery.empty(w.end(), limits));
                }

                final long estimate = aggregation.estimate(range);

                if (estimate >= 0 && aggregationLimit.isGreater(estimate)) {
                    return async.resolved(FullQuery.error(w.end(), QueryError.fromMessage(
                        String.format(
                            "aggregation is estimated more points [%d/%d] than what is allowed",
                            estimate, aggregationLimit.asLong().get()))));
                }

                final AggregationSession session = aggregation.session(range);

                final List<Callable<AsyncFuture<Pair<Series, FetchData>>>> fetches =
                    new ArrayList<>();

                /* setup fetches */
                accept(b -> {
                    for (final Series s : result.getSeries()) {
                        fetches.add(() -> b
                            .fetch(new FetchData.Request(source, s, range, options), watcher)
                            .directTransform(d -> Pair.of(s, d)));
                    }
                });

                /* setup collector */

                final ResultCollector collector;

                if (options.isTracing()) {
                    // tracing enabled, keeps track of each individual FetchData trace.
                    collector = new ResultCollector(watcher, aggregation, session, limits,
                        options.getGroupLimit().orElse(groupLimit), failOnLimits) {
                        final ConcurrentLinkedQueue<QueryTrace> traces =
                            new ConcurrentLinkedQueue<>();

                        @Override
                        public void resolved(Pair<Series, FetchData> result) throws Exception {
                            traces.add(result.getRight().getTrace());
                            super.resolved(result);
                        }

                        @Override
                        public QueryTrace buildTrace() {
                            return w.end(ImmutableList.copyOf(traces));
                        }
                    };
                } else {
                    // very limited tracing, does not collected each individual FetchData trace.
                    collector = new ResultCollector(watcher, aggregation, session, limits,
                        options.getGroupLimit().orElse(groupLimit), failOnLimits) {
                        @Override
                        public QueryTrace buildTrace() {
                            return w.end();
                        }
                    };
                }

                return async.eventuallyCollect(fetches, collector, fetchParallelism);
            };

            return metadata
                .findSeries(new FindSeries.Request(filter, range, seriesLimit))
                .onDone(reporter.reportFindSeries())
                .lazyTransform(transform)
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
        public AsyncFuture<FetchData> fetch(
            final FetchData.Request request, final FetchQuotaWatcher watcher
        ) {
            final List<AsyncFuture<FetchData>> callbacks = map(b -> b.fetch(request, watcher));
            return async.collect(callbacks, FetchData.collect(FETCH));
        }

        @Override
        public AsyncFuture<FetchData> fetch(final FetchData.Request request) {
            return fetch(request, FetchQuotaWatcher.NO_QUOTA);
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
        implements StreamCollector<Pair<Series, FetchData>, FullQuery> {
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        final FetchQuotaWatcher watcher;
        final AggregationInstance aggregation;
        final AggregationSession session;
        final ResultLimits limits;
        final OptionalLimit groupLimit;
        final boolean failOnLimits;

        @Override
        public void resolved(final Pair<Series, FetchData> result) throws Exception {
            final FetchData f = result.getRight();

            for (final MetricCollection g : f.getGroups()) {
                g.updateAggregation(session, result.getLeft().getTags(),
                    ImmutableSet.of(result.getLeft()));
            }
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

            if (watcher.isQuotaViolated()) {
                final List<RequestError> errors = checkIssues(failed, cancelled)
                    .map(QueryError::fromMessage)
                    .map(ImmutableList::of)
                    .orElseGet(ImmutableList::of);

                return new FullQuery(trace, errors, ImmutableList.of(), Statistics.empty(),
                    limits.add(ResultLimit.QUOTA));
            }

            checkIssues(failed, cancelled).map(RuntimeException::new).ifPresent(e -> {
                for (final Throwable t : errors) {
                    e.addSuppressed(t);
                }

                throw e;
            });

            final AggregationResult result = session.result();

            final List<ResultGroup> groups = new ArrayList<>();

            final ImmutableSet.Builder<ResultLimit> limits =
                ImmutableSet.<ResultLimit>builder().addAll(this.limits.getLimits());

            for (final AggregationOutput group : result.getResult()) {
                if (groupLimit.isGreaterOrEqual(groups.size())) {
                    if (failOnLimits) {
                        final List<RequestError> errors = ImmutableList.of(QueryError.fromMessage(
                            "The number of result groups is more than the allowed limit of " +
                                groupLimit));

                        return new FullQuery(trace, errors, ImmutableList.of(), Statistics.empty(),
                            new ResultLimits(limits.add(ResultLimit.GROUP).build()));
                    }

                    limits.add(ResultLimit.GROUP);
                    break;
                }

                groups.add(new ResultGroup(group.getKey(), group.getSeries(), group.getMetrics(),
                    aggregation.cadence()));
            }

            // Gather statistics and save in QueryTrace
            final Map<String, Long> statisticsCounters = result.getStatistics().getCounters();
            final long preAggregationSampleSize = statisticsCounters.getOrDefault(
                "Aggregation.sampleSize", 0L);
            long numSeries = 0;
            for (ResultGroup g : groups) {
                numSeries += g.getSeries().size();
            }
            trace.setPreAggregationSampleSize(preAggregationSampleSize);
            trace.setNumSeries(numSeries);

            return new FullQuery(trace, ImmutableList.of(), groups, result.getStatistics(),
                new ResultLimits(limits.build()));
        }

        private Optional<String> checkIssues(final int failed, final int cancelled) {
            if (failed > 0 || cancelled > 0) {
                return Optional.of(
                    "Some fetches failed (" + failed + ") or were cancelled (" + cancelled + ")");
            }

            return Optional.empty();
        }
    }
}
