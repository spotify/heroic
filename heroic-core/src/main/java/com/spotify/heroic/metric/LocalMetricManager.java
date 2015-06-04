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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.inject.Inject;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.NotImplementedException;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.GroupAggregation;
import com.spotify.heroic.exceptions.BackendGroupException;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metric.async.AggregatedCallbackStream;
import com.spotify.heroic.metric.async.SimpleCallbackStream;
import com.spotify.heroic.metric.model.BackendEntry;
import com.spotify.heroic.metric.model.BackendKey;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.ResultGroups;
import com.spotify.heroic.metric.model.TagValues;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;
import com.spotify.heroic.statistics.MetricBackendGroupReporter;
import com.spotify.heroic.utils.BackendGroups;
import com.spotify.heroic.utils.GroupMember;
import com.spotify.heroic.utils.SelectedGroup;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.StreamCollector;

@Slf4j
@ToString(of = {})
public class LocalMetricManager implements MetricManager {
    public static FetchQuotaWatcher NO_QUOTA_WATCHER = new FetchQuotaWatcher() {
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
    private final int groupFetchParallelism;

    /**
     * @param groupLimit
     *            The maximum amount of groups this manager will allow to be generated.
     * @param groupLoadLimit
     *            The maximum amount of series a single group may contain.
     * @param seriesLimit
     *            The maximum amount of series in total an entire query may use.
     */
    public LocalMetricManager(final int groupLimit, final int seriesLimit, final long aggregationLimit,
            final long dataLimit, final int groupFetchParallelism) {
        this.groupLimit = groupLimit;
        this.seriesLimit = seriesLimit;
        this.aggregationLimit = aggregationLimit;
        this.dataLimit = dataLimit;
        this.groupFetchParallelism = groupFetchParallelism;
    }

    @Inject
    private BackendGroups<MetricBackend> backends;

    @Inject
    private MetadataManager metadata;

    @Inject
    private AsyncFramework async;

    @Inject
    private MetricBackendGroupReporter reporter;

    @Override
    public List<GroupMember<MetricBackend>> getBackends() {
        return backends.all();
    }

    @Override
    public MetricBackendGroup useDefaultGroup() throws BackendGroupException {
        return new MetricBackendGroupImpl(backends.useDefault(), metadata.useDefaultGroup());
    }

    @Override
    public MetricBackendGroup useGroup(final String group) throws BackendGroupException {
        return new MetricBackendGroupImpl(backends.use(group), metadata.useDefaultGroup());
    }

    @Override
    public MetricBackendGroup useGroups(Set<String> groups) throws BackendGroupException {
        return new MetricBackendGroupImpl(backends.use(groups), metadata.useGroups(groups));
    }

    @RequiredArgsConstructor
    private class MetricBackendGroupImpl implements MetricBackendGroup {
        private final SelectedGroup<MetricBackend> backends;
        private final MetadataBackend metadata;

        @Override
        public Set<String> getGroups() {
            return backends.groups();
        }

        @Override
        public <T extends TimeData> AsyncFuture<ResultGroups> query(Class<T> source, final Filter filter,
                final List<String> groupBy, final DateRange range, Aggregation aggregation, final boolean noCache) {
            final Aggregation nested;

            if (groupBy != null) {
                nested = new GroupAggregation(groupBy, aggregation);
            } else {
                nested = aggregation;
            }

            final FetchQuotaWatcher watcher = new LimitedFetchQuotaWatcher(dataLimit);
            final QueryOperation op = buildOperation(source, filter, range, noCache, nested, watcher);

            /* groupLoadLimit + 1, so that we return one too many results when more than groupLoadLimit series are
             * available. This will cause the query engine to reject the request because of too large group. */
            final RangeFilter rangeFilter = RangeFilter.filterFor(filter, range, seriesLimit + 1);

            try {
                return metadata.findSeries(rangeFilter).onAny(reporter.reportFindSeries())
                        .transform(runQueries(op, nested, watcher)).onAny(reporter.reportQueryMetrics());
            } catch (Exception e) {
                return async.failed(e);
            }
        }

        @Override
        public <T extends TimeData> AsyncFuture<FetchData<T>> fetch(final Class<T> source, final Series series,
                final DateRange range, final FetchQuotaWatcher watcher) {
            final List<AsyncFuture<FetchData<T>>> callbacks = new ArrayList<>();

            run(new InternalOperation() {
                @Override
                public void run(int disabled, MetricBackend backend) throws Exception {
                    callbacks.add(backend.fetch(source, series, range, watcher));
                }
            });

            return async.collect(callbacks, FetchData.<T> merger(series));
        }

        @Override
        public <T extends TimeData> AsyncFuture<FetchData<T>> fetch(final Class<T> source, final Series series,
                final DateRange range) {
            return fetch(source, series, range, NO_QUOTA_WATCHER);
        }

        private <T extends TimeData> AsyncFuture<ResultGroups> fetchAll(final Class<T> source,
                final List<TagValues> group, final Filter filter, final Set<Series> series, final DateRange range,
                final Aggregation aggregation, final boolean disableCache, final FetchQuotaWatcher watcher) {
            final List<AsyncFuture<ResultGroups>> callbacks = new ArrayList<>();

            run(new InternalOperation() {
                @Override
                public void run(final int disabled, final MetricBackend backend) throws Exception {
                    callbacks.add(fetch(source, group, backend, series, range, aggregation, watcher).error(
                            ResultGroups.seriesError(group)));
                }
            });

            return async.collect(callbacks, ResultGroups.merger()).onAny(reporter.reportQuery());
        }

        @Override
        public AsyncFuture<WriteResult> write(final WriteMetric write) {
            final List<AsyncFuture<WriteResult>> callbacks = new ArrayList<>();

            run(new InternalOperation() {
                @Override
                public void run(int disabled, MetricBackend backend) throws Exception {
                    callbacks.add(backend.write(write));
                }
            });

            return async.collect(callbacks, WriteResult.merger()).onAny(reporter.reportWrite());
        }

        /**
         * Perform a direct write on available configured backends.
         *
         * @param writes
         *            Batch of writes to perform.
         * @return A callback indicating how the writes went.
         * @throws MetricBackendException
         * @throws BackendGroupException
         */
        @Override
        public AsyncFuture<WriteResult> write(final Collection<WriteMetric> writes) {
            final List<AsyncFuture<WriteResult>> callbacks = new ArrayList<>();

            run(new InternalOperation() {
                @Override
                public void run(int disabled, MetricBackend backend) throws Exception {
                    callbacks.add(backend.write(writes));
                }
            });

            return async.collect(callbacks, WriteResult.merger()).onAny(reporter.reportWriteBatch());
        }

        @Override
        public AsyncFuture<List<BackendKey>> keys(final BackendKey start, final BackendKey end, final int limit) {
            final List<AsyncFuture<List<BackendKey>>> callbacks = new ArrayList<>();

            run(new InternalOperation() {
                @Override
                public void run(int disabled, MetricBackend backend) throws Exception {
                    callbacks.add(backend.keys(start, end, limit));
                }
            });

            return async.collect(callbacks, BackendKey.merge());
        }

        @Override
        public boolean isReady() {
            for (final MetricBackend backend : backends) {
                if (!backend.isReady())
                    return false;
            }

            return true;
        }

        @Override
        public Iterable<BackendEntry> listEntries() {
            throw new NotImplementedException("not supported");
        }

        private <T extends TimeData> QueryOperation buildOperation(final Class<T> source, final Filter filter,
                final DateRange range, final boolean noCache, final Aggregation nested, final FetchQuotaWatcher watcher) {
            return new QueryOperation() {
                @Override
                public AsyncFuture<ResultGroups> execute(List<TagValues> group, Set<Series> series) {
                    return fetchAll(source, group, filter, series, range, nested, noCache, watcher);
                }
            };
        }

        private LazyTransform<FindSeries, ResultGroups> runQueries(final QueryOperation op,
                final Aggregation aggregation, final FetchQuotaWatcher watcher) {
            return new LazyTransform<FindSeries, ResultGroups>() {
                @Override
                public AsyncFuture<ResultGroups> transform(final FindSeries result) throws Exception {
                    if (result.getSize() >= seriesLimit)
                        throw new IllegalArgumentException("The total number of series fetched " + result.getSize()
                                + " would exceed the allowed limit of " + seriesLimit);

                    final Map<List<TagValues>, Set<Series>> groups = setupMetricGroups(aggregation, result.getSeries());

                    if (groups.size() > groupLimit)
                        throw new IllegalArgumentException("The current query is too heavy! (More than " + groupLimit
                                + " timeseries would be sent to your browser).");

                    final List<Callable<AsyncFuture<ResultGroups>>> futures = new ArrayList<>();

                    for (final Map.Entry<List<TagValues>, Set<Series>> entry : groups.entrySet()) {
                        final List<TagValues> key = entry.getKey();
                        final Set<Series> series = entry.getValue();

                        if (series.isEmpty())
                            continue;

                        futures.add(new Callable<AsyncFuture<ResultGroups>>() {
                            @Override
                            public AsyncFuture<ResultGroups> call() throws Exception {
                                return op.execute(key, series);
                            }
                        });
                    }

                    return async.eventuallyCollect(futures, collectResultGroups(watcher), groupFetchParallelism);
                }

                private StreamCollector<ResultGroups, ResultGroups> collectResultGroups(final FetchQuotaWatcher watcher) {
                    return new StreamCollector<ResultGroups, ResultGroups>() {
                        final ConcurrentLinkedQueue<ResultGroups> results = new ConcurrentLinkedQueue<>();

                        @Override
                        public void resolved(ResultGroups result) throws Exception {
                            results.add(result);
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
                            if (failed > 0 || cancelled > 0) {
                                final String message = String.format(
                                        "Some result groups failed (%d) or were cancelled (%d)", failed, cancelled);

                                if (watcher.isQuotaViolated())
                                    throw new Exception(message + " (fetch quota was reached)");

                                throw new Exception(message);
                            }

                            return ResultGroups.merge(results);
                        }
                    };
                }
            };
        }

        private Map<List<TagValues>, Set<Series>> setupMetricGroups(Aggregation aggregation, final Set<Series> series) {
            final List<Aggregation.TraverseState> states = aggregation.traverse(convert(series));

            final Map<List<TagValues>, Set<Series>> results = new HashMap<>();

            int total = 0;

            for (final Aggregation.TraverseState s : states) {
                total += s.getSeries().size();
                results.put(key(s.getSeries()), s.getSeries());
            }

            if (total != series.size())
                throw new IllegalStateException("traverse stage must only map the number of series, not change it");

            return results;
        }

        private List<Aggregation.TraverseState> convert(Set<Series> series) {
            final List<Aggregation.TraverseState> groups = new ArrayList<>(series.size());

            for (final Series s : series)
                groups.add(new Aggregation.TraverseState(s.getTags(), ImmutableSet.of(s)));

            return groups;
        }

        private <T extends TimeData> AsyncFuture<ResultGroups> fetch(Class<T> source, List<TagValues> group,
                MetricBackend backend, Set<Series> series, final DateRange range, Aggregation aggregation,
                final FetchQuotaWatcher watcher) {
            final DateRange modified = aggregateRange(range, aggregation);
            final List<AsyncFuture<FetchData<T>>> fetches = new ArrayList<>();

            for (final Series serie : series)
                fetches.add(backend.fetch(source, serie, modified, watcher));

            return async.collect(fetches, aggregationReducer(source, group, range, aggregation, fetches));
        }

        /**
         * Return a modified range if an aggregation is specified.
         *
         * Aggregations require a range to get modified according to their extent in order to 'fetch' datapoints for the
         * entire aggregation range.
         *
         * @param range
         *            The range to modify.
         * @param aggregation
         *            The aggregation to use for the modification.
         * @return The original range if no aggregation is specified, otherwise a modified one.
         */
        private DateRange aggregateRange(final DateRange range, final Aggregation aggregation) {
            final Sampling sampling = aggregation.sampling();

            if (sampling == null)
                return range;

            return range.shiftStart(-sampling.getExtent());
        }

        private <T extends TimeData> StreamCollector<FetchData<T>, ResultGroups> aggregationReducer(Class<T> in,
                List<TagValues> group, final DateRange range, final Aggregation aggregation,
                final List<? extends AsyncFuture<?>> fetches) {
            if (aggregation.sampling() == null)
                return new SimpleCallbackStream<T>(in);

            final Class<?> expected = aggregation.input();

            if (!matches(expected, in))
                throw new IllegalArgumentException("input of aggregation does not match [" + expected + " != " + in
                        + "]");

            final long estimate = aggregation.estimate(range);

            if (estimate != -1 && estimate > aggregationLimit)
                throw new IllegalArgumentException(String.format("aggregation would result in more than %d datapoints",
                        aggregationLimit));

            final Aggregation.Session session = aggregation.session(in, range);
            return new AggregatedCallbackStream<T>(group, session, fetches);
        }

        private boolean matches(Class<?> expected, Class<?> actual) {
            if (expected == null)
                return true;

            return expected.isAssignableFrom(actual);
        }

        private void run(InternalOperation op) {
            if (backends.isEmpty())
                throw new IllegalStateException("cannot run operation; no backends available for given group");

            for (final MetricBackend b : backends) {
                try {
                    op.run(backends.getDisabled(), b);
                } catch (final Exception e) {
                    throw new RuntimeException("setting up backend operation failed", e);
                }
            }
        }
    }

    private final Comparator<String> COMPARATOR = new Comparator<String>() {
        @Override
        public int compare(String a, String b) {
            if (a == null) {
                if (b == null)
                    return 0;

                return -1;
            }

            if (b == null)
                return 1;

            return a.compareTo(b);
        }
    };

    private List<TagValues> key(Set<Series> series) {
        final Map<String, SortedSet<String>> key = new HashMap<>();

        for (final Series s : series) {
            for (final Map.Entry<String, String> e : s.getTags().entrySet()) {
                SortedSet<String> values = key.get(e.getKey());

                if (values == null) {
                    values = new TreeSet<String>(COMPARATOR);
                    key.put(e.getKey(), values);
                }

                values.add(e.getValue());
            }
        }

        final List<TagValues> tags = new ArrayList<>(key.size());

        for (final Map.Entry<String, SortedSet<String>> e : key.entrySet())
            tags.add(new TagValues(e.getKey(), new ArrayList<>(e.getValue())));

        return tags;
    }

    private static interface InternalOperation {
        void run(int disabled, MetricBackend backend) throws Exception;
    }

    private static interface QueryOperation {
        AsyncFuture<ResultGroups> execute(List<TagValues> group, Set<Series> series);
    }
}
