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

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.AggregationData;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationResult;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.aggregation.AggregationTraversal;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.BackendGroups;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.GroupMember;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.SelectedGroup;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.statistics.MetricBackendGroupReporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.NotImplementedException;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Collector;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.StreamCollector;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@ToString(of = {})
public class LocalMetricManager implements MetricManager {
    private static final QueryTrace.Identifier QUERY =
            QueryTrace.identifier(LocalMetricManager.class, "query");
    private static final QueryTrace.Identifier FETCH =
            QueryTrace.identifier(LocalMetricManager.class, "fetch");
    private static final QueryTrace.Identifier KEYS =
            QueryTrace.identifier(LocalMetricManager.class, "keys");

    public static final FetchQuotaWatcher NO_QUOTA_WATCHER = new FetchQuotaWatcher() {
        @Override
        public boolean readData(long n) {
            return true;
        }

        @Override
        public boolean mayReadData() {
            return true;
        }

        @Override
        public int getReadDataQuota() {
            return Integer.MAX_VALUE;
        }

        @Override
        public boolean isQuotaViolated() {
            return false;
        }
    };

    private final int groupLimit;
    private final int seriesLimit;
    private final long aggregationLimit;
    private final long dataLimit;
    private final int fetchParallelism;

    private final AsyncFramework async;
    private final BackendGroups<MetricBackend> backends;
    private final MetadataManager metadata;
    private final MetricBackendGroupReporter reporter;

    /**
     * @param groupLimit The maximum amount of groups this manager will allow to be generated.
     * @param seriesLimit The maximum amount of series in total an entire query may use.
     * @param aggregationLimit The maximum number of (estimated) data points a single aggregation
     *            may produce.
     * @param dataLimit The maximum number of samples a single query is allowed to fetch.
     * @param fetchParallelism How many fetches that are allowed to be performed in parallel.
     */
    public LocalMetricManager(final int groupLimit, final int seriesLimit,
            final long aggregationLimit, final long dataLimit, final int fetchParallelism,
            final AsyncFramework async, final BackendGroups<MetricBackend> backends,
            final MetadataManager metadata, final MetricBackendGroupReporter reporter) {
        this.groupLimit = groupLimit;
        this.seriesLimit = seriesLimit;
        this.aggregationLimit = aggregationLimit;
        this.dataLimit = dataLimit;
        this.fetchParallelism = fetchParallelism;
        this.async = async;
        this.backends = backends;
        this.metadata = metadata;
        this.reporter = reporter;
    }

    @Override
    public List<MetricBackend> allMembers() {
        return backends.allMembers();
    }

    @Override
    public List<MetricBackend> use(String group) {
        return backends.use(group).getMembers();
    }

    @Override
    public List<GroupMember<MetricBackend>> getBackends() {
        return backends.all();
    }

    @Override
    public MetricBackendGroup useDefaultGroup() {
        return new MetricBackendGroupImpl(backends.useDefault(), metadata.useDefaultGroup());
    }

    @Override
    public MetricBackendGroup useGroup(final String group) {
        return new MetricBackendGroupImpl(backends.use(group), metadata.useDefaultGroup());
    }

    @Override
    public MetricBackendGroup useGroups(Set<String> groups) {
        return new MetricBackendGroupImpl(backends.use(groups), metadata.useGroups(groups));
    }

    private class MetricBackendGroupImpl extends AbstractMetricBackend
            implements MetricBackendGroup {
        private final SelectedGroup<MetricBackend> backends;
        private final MetadataBackend metadata;

        public MetricBackendGroupImpl(final SelectedGroup<MetricBackend> backends,
                final MetadataBackend metadata) {
            super(async);
            this.backends = backends;
            this.metadata = metadata;
        }

        @Override
        public Groups getGroups() {
            return backends.groups();
        }

        @Override
        public boolean isEmpty() {
            return backends.isEmpty();
        }

        @Override
        public int size() {
            return backends.size();
        }

        @Override
        public AsyncFuture<ResultGroups> query(MetricType source, final Filter filter,
                final DateRange range, final AggregationInstance aggregation,
                final QueryOptions options) {
            final FetchQuotaWatcher watcher = new LimitedFetchQuotaWatcher(dataLimit);

            /* groupLoadLimit + 1, so that we return one too many results when more than
             * groupLoadLimit series are available. This will cause the query engine to reject the
             * request because of too large group. */
            final RangeFilter rangeFilter =
                    RangeFilter.filterFor(filter, Optional.ofNullable(range), seriesLimit + 1);

            final LazyTransform<FindSeries, ResultGroups> transform = (final FindSeries result) -> {
                /* if empty, there are not time series on this shard */
                if (result.isEmpty()) {
                    return async.resolved(ResultGroups.empty(QUERY));
                }

                if (result.getSize() >= seriesLimit) {
                    throw new IllegalArgumentException(
                            "The total number of series fetched " + result.getSize()
                                    + " would exceed the allowed limit of " + seriesLimit);
                }

                final long estimate = aggregation.estimate(range);

                if (estimate > aggregationLimit) {
                    throw new IllegalArgumentException(String.format(
                            "aggregation is estimated more points [%d/%d] than what is allowed",
                            estimate, aggregationLimit));
                }

                final AggregationTraversal traversal =
                        aggregation.session(states(result.getSeries()), range);

                if (traversal.getStates().size() > groupLimit) {
                    throw new IllegalArgumentException("The current query is too heavy! (More than "
                            + groupLimit + " timeseries would be sent to your client).");
                }

                final AggregationSession session = traversal.getSession();

                final List<Callable<AsyncFuture<FetchData>>> fetches = new ArrayList<>();

                /* setup fetches */

                for (final AggregationState state : traversal.getStates()) {
                    final Set<Series> series = state.getSeries();

                    if (series.isEmpty()) {
                        continue;
                    }

                    runVoid(b -> {
                        for (final Series serie : series) {
                            fetches.add(() -> {
                                if (watcher.isQuotaViolated()) {
                                    throw new IllegalStateException("quota limit violated");
                                }

                                return b.fetch(source, serie, range, watcher, options);
                            });
                        }

                        return null;
                    });
                }

                /* setup collector */

                final StreamCollector<FetchData, ResultGroups> collector;

                final Stopwatch w = Stopwatch.createStarted();

                if (options.isTracing()) {
                    // tracing enabled, keeps track of each individual FetchData trace.
                    collector = new ResultCollector(watcher, aggregation, session) {
                        final ConcurrentLinkedQueue<QueryTrace> traces =
                                new ConcurrentLinkedQueue<>();

                        @Override
                        public void resolved(FetchData result) throws Exception {
                            traces.add(result.getTrace());
                            super.resolved(result);
                        }

                        @Override
                        public QueryTrace buildTrace() {
                            return new QueryTrace(QUERY, w.elapsed(TimeUnit.NANOSECONDS),
                                    ImmutableList.copyOf(traces));
                        }
                    };
                } else {
                    // very limited tracing, does not collected each individual FetchData trace.
                    collector = new ResultCollector(watcher, aggregation, session) {
                        @Override
                        public QueryTrace buildTrace() {
                            return new QueryTrace(QUERY, w.elapsed(TimeUnit.NANOSECONDS));
                        }
                    };
                }

                return async.eventuallyCollect(fetches, collector, fetchParallelism);
            };

            return metadata.findSeries(rangeFilter).onDone(reporter.reportFindSeries())
                    .lazyTransform(transform).onDone(reporter.reportQueryMetrics());
        }

        @Override
        public Statistics getStatistics() {
            Statistics result = Statistics.empty();

            for (final Statistics s : run(b -> b.getStatistics())) {
                result = result.merge(s);
            }

            return result;
        }

        @Override
        public AsyncFuture<FetchData> fetch(final MetricType source, final Series series,
                final DateRange range, final FetchQuotaWatcher watcher,
                final QueryOptions options) {
            final List<AsyncFuture<FetchData>> callbacks =
                    run(b -> b.fetch(source, series, range, watcher, options));
            return async.collect(callbacks, FetchData.collect(FETCH, series));
        }

        @Override
        public AsyncFuture<FetchData> fetch(final MetricType source, final Series series,
                final DateRange range, final QueryOptions options) {
            return fetch(source, series, range, NO_QUOTA_WATCHER, options);
        }

        @Override
        public AsyncFuture<WriteResult> write(final WriteMetric write) {
            return async.collect(run(b -> b.write(write)), WriteResult.merger())
                    .onDone(reporter.reportWrite());
        }

        /**
         * Perform a direct write on available configured backends.
         *
         * @param writes Batch of writes to perform.
         * @return A callback indicating how the writes went.
         * @throws MetricBackendException
         * @throws BackendGroupException
         */
        @Override
        public AsyncFuture<WriteResult> write(final Collection<WriteMetric> writes) {
            return async.collect(run(b -> b.write(writes)), WriteResult.merger())
                    .onDone(reporter.reportWriteBatch());
        }

        @Override
        public AsyncObservable<BackendKeySet> streamKeys(final BackendKeyFilter filter,
                final QueryOptions options) {
            return AsyncObservable.chain(run(b -> b.streamKeys(filter, options)));
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
            return async.collectAndDiscard(run(b -> b.configure()));
        }

        @Override
        public AsyncFuture<List<String>> serializeKeyToHex(BackendKey key) {
            return async.collect(run(b -> b.serializeKeyToHex(key))).directTransform(result -> {
                return ImmutableList.copyOf(Iterables.concat(result));
            });
        }

        @Override
        public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
            return async.collect(run(b -> b.deserializeKeyFromHex(key))).directTransform(result -> {
                return ImmutableList.copyOf(Iterables.concat(result));
            });
        }

        @Override
        public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
            return async.collectAndDiscard(run(b -> b.deleteKey(key, options)));
        }

        @Override
        public AsyncFuture<Long> countKey(BackendKey key, QueryOptions options) {
            return async.collect(run(b -> b.countKey(key, options))).directTransform(result -> {
                long count = 0;

                for (final long c : result) {
                    count += c;
                }

                return count;
            });
        }

        @Override
        public AsyncFuture<MetricCollection> fetchRow(final BackendKey key) {
            final List<AsyncFuture<MetricCollection>> callbacks = run(b -> b.fetchRow(key));

            return async.collect(callbacks, new Collector<MetricCollection, MetricCollection>() {
                @Override
                public MetricCollection collect(Collection<MetricCollection> results)
                        throws Exception {
                    final List<List<? extends Metric>> collections = new ArrayList<>();

                    for (final MetricCollection result : results) {
                        collections.add(result.getData());
                    }

                    return MetricCollection.mergeSorted(key.getType(), collections);
                }
            });
        }

        @Override
        public AsyncObservable<MetricCollection> streamRow(final BackendKey key) {
            return AsyncObservable.chain(run(b -> b.streamRow(key)));
        }

        private void runVoid(InternalOperation<Void> op) {
            for (final MetricBackend b : backends.getMembers()) {
                try {
                    op.run(b);
                } catch (final Exception e) {
                    throw new RuntimeException("setting up backend operation failed", e);
                }
            }
        }

        private <T> List<T> run(InternalOperation<T> op) {
            final ImmutableList.Builder<T> result = ImmutableList.builder();

            for (final MetricBackend b : backends) {
                try {
                    result.add(op.run(b));
                } catch (final Exception e) {
                    throw new RuntimeException("setting up backend operation failed", e);
                }
            }

            return result.build();
        }
    }

    private static List<AggregationState> states(Set<Series> series) {
        return ImmutableList.copyOf(series.stream()
                .map(s -> new AggregationState(s.getTags(), ImmutableSet.of(s))).iterator());
    }

    @RequiredArgsConstructor
    private abstract static class ResultCollector
            implements StreamCollector<FetchData, ResultGroups> {
        final FetchQuotaWatcher watcher;
        final AggregationInstance aggregation;
        final AggregationSession session;

        @Override
        public void resolved(FetchData result) throws Exception {
            for (final MetricCollection g : result.getGroups()) {
                g.updateAggregation(session, result.getSeries().getTags(),
                        ImmutableSet.of(result.getSeries()));
            }
        }

        @Override
        public void failed(Throwable cause) throws Exception {
            log.error("Fetch failed", cause);
        }

        @Override
        public void cancelled() throws Exception {
        }

        @Override
        public ResultGroups end(int resolved, int failed, int cancelled) throws Exception {
            return collect(resolved, failed, cancelled, buildTrace());
        }

        public abstract QueryTrace buildTrace();

        private ResultGroups collect(int resolved, int failed, int cancelled,
                final QueryTrace trace) throws Exception {
            if (failed > 0 || cancelled > 0) {
                final String message = String.format(
                        "Some result groups failed (%d) or were cancelled (%d)", failed, cancelled);

                if (watcher.isQuotaViolated()) {
                    throw new Exception(message + " (fetch quota was reached)");
                }

                throw new Exception(message);
            }

            final AggregationResult result = session.result();

            final List<ResultGroup> groups = new ArrayList<>();

            for (final AggregationData group : result.getResult()) {
                /* skip empty groups (no valid values) */
                if (group.isEmpty()) {
                    continue;
                }

                final List<TagValues> g = TagValues.fromEntries(
                        Iterators.concat(Iterators.transform(group.getSeries().iterator(),
                                s -> s.getTags().entrySet().iterator())));

                groups.add(new ResultGroup(g, group.getMetrics(), aggregation.cadence()));
            }

            final Statistics stat = result.getStatistics().merge(
                    Statistics.of(MetricManager.RESOLVED, resolved, MetricManager.FAILED, failed));

            return new ResultGroups(groups, ImmutableList.of(), stat, trace);
        }
    }

    private static interface InternalOperation<T> {
        T run(MetricBackend backend) throws Exception;
    }
}
