package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Getter;

import com.codahale.metrics.MetricRegistry;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.Stream;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.backend.list.QueryGroup;
import com.spotify.heroic.backend.list.QuerySingle;
import com.spotify.heroic.backend.list.RowGroups;
import com.spotify.heroic.backend.model.FindRowGroups;
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.backend.model.GetAllRowsResult;
import com.spotify.heroic.backend.model.GroupedAllRowsResult;
import com.spotify.heroic.backend.model.RangedQuery;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.query.MetricsQuery;

public class ListBackendManager implements BackendManager {
    @Getter
    private final List<MetricBackend> metricBackends;

    @Getter
    private final List<EventBackend> eventBackends;

    @Getter
    private final long maxAggregationMagnitude;

    private final QuerySingle querySingle;
    private final QueryGroup queryGroup;

    public ListBackendManager(List<Backend> backends, MetricRegistry registry,
            long maxAggregationMagnitude, long maxQueriableDataPoints,
            AggregationCache cache) {
        this.metricBackends = filterMetricBackends(backends);
        this.eventBackends = filterEventBackends(backends);
        this.maxAggregationMagnitude = maxAggregationMagnitude;

        this.querySingle = new QuerySingle(metricBackends,
                maxQueriableDataPoints, cache);
        this.queryGroup = new QueryGroup(metricBackends, cache);
    }

    private List<EventBackend> filterEventBackends(List<Backend> backends) {
        final List<EventBackend> eventBackends = new ArrayList<EventBackend>();

        for (final Backend backend : backends) {
            if (backend instanceof EventBackend)
                eventBackends.add((EventBackend) backend);
        }

        return eventBackends;
    }

    private List<MetricBackend> filterMetricBackends(List<Backend> backends) {
        final List<MetricBackend> metricBackends = new ArrayList<MetricBackend>();

        for (final Backend backend : backends) {
            if (backend instanceof MetricBackend)
                metricBackends.add((MetricBackend) backend);
        }

        return metricBackends;
    }

    private <T> List<T> optionalEmptyList(List<T> list) {
        if (list == null)
            return new ArrayList<T>();

        return list;
    }

    @Override
    public Callback<QueryMetricsResult> queryMetrics(final MetricsQuery query)
            throws QueryException {
        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final DateRange range = query.getRange().buildDateRange();
        final List<Aggregation> definitions = optionalEmptyList(query.getAggregators());

        if (key == null || key.isEmpty())
            throw new QueryException("'key' must be defined");

        if (range == null)
            throw new QueryException("Range must be specified");

        if (!(range.start() < range.end()))
            throw new QueryException("Range start must come before its end");

        final AggregationGroup aggregation = new AggregationGroup(definitions);
        final AggregatorGroup aggregator = aggregation.build();

        final long memoryMagnitude = aggregator
                .getCalculationMemoryMagnitude(range);

        if (memoryMagnitude > maxAggregationMagnitude) {
            throw new QueryException(
                    "This query would result in too many datapoints");
        }

        final DateRange rounded = roundRange(aggregator, range);

        if (groupBy != null && !groupBy.isEmpty()) {
            final FindRowGroups criteria = new FindRowGroups(key, rounded,
                    tags, groupBy);
            return queryGroup.execute(criteria, aggregator);
        }

        final FindRows criteria = new FindRows(key, rounded, tags);
        return querySingle.execute(criteria, aggregator);
    }

    @Override
    public void streamMetrics(MetricsQuery query, Stream.Handle<QueryMetricsResult> handle)
            throws QueryException {
        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final List<Aggregation> definitions = optionalEmptyList(query.getAggregators());

        if (key == null || key.isEmpty())
            throw new QueryException("'key' must be defined");

        final AggregationGroup aggregation = new AggregationGroup(definitions);
        final AggregatorGroup aggregator = aggregation.build();

        final DateRange range = query.getRange().buildDateRange();
        final DateRange rounded = roundRange(aggregator, range);

        if (groupBy != null && !groupBy.isEmpty()) {
            final FindRowGroups criteria = new FindRowGroups(key, rounded, tags, groupBy);
            final Callback<RowGroups> rowGroups = queryGroup.findRowGroups(criteria);

            stream(criteria, criteria.withRange(rounded.withStart(rounded.end())), aggregation, handle,
                    new StreamingQuery<FindRowGroups>() {
                @Override
                public Callback<QueryMetricsResult> query(FindRowGroups current, AggregatorGroup aggregator) {
                    return rowGroups.transform(queryGroup.buildTransformer(current, aggregator));
                }
            });

            return;
        }

        final FindRows criteria = new FindRows(key, rounded, tags);
        stream(criteria, criteria.withRange(rounded.withStart(rounded.end())), aggregation, handle,
                new StreamingQuery<FindRows>() {
            @Override
            public Callback<QueryMetricsResult> query(FindRows current, AggregatorGroup aggregator) {
                return querySingle.execute(current, aggregator);
            }
        });
    }

    @Override
    public Callback<GroupedAllRowsResult> getAllRows() {
        final List<Callback<GetAllRowsResult>> backendRequests = new ArrayList<Callback<GetAllRowsResult>>();
        final Callback<GroupedAllRowsResult> overallCallback = new ConcurrentCallback<GroupedAllRowsResult>();

        for (final MetricBackend backend : metricBackends) {
            backendRequests.add(backend.getAllRows());
        }

        final Callback.Reducer<GetAllRowsResult, GroupedAllRowsResult> resultReducer = new Callback.Reducer<GetAllRowsResult, GroupedAllRowsResult>() {
            @Override
            public GroupedAllRowsResult done(
                    Collection<GetAllRowsResult> results,
                    Collection<Throwable> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                final Set<TimeSerie> result = new HashSet<TimeSerie>();

                for (final GetAllRowsResult backendResult : results) {
                    final Map<String, List<DataPointsRowKey>> rows = backendResult
                            .getRows();

                    for (final Map.Entry<String, List<DataPointsRowKey>> entry : rows
                            .entrySet()) {
                        for (final DataPointsRowKey rowKey : entry.getValue()) {
                            result.add(new TimeSerie(rowKey.getMetricName(),
                                    rowKey.getTags()));
                        }
                    }
                }

                return new GroupedAllRowsResult(result);
            }
        };

        return overallCallback.reduce(backendRequests,
                resultReducer);
    }

    /**
     * Check if the query wants to hint at a specific interval. If that is the
     * case, round the provided date to the specified interval.
     * 
     * @param query
     * @return
     */
    private DateRange roundRange(AggregatorGroup aggregator, DateRange range) {
        final long hint = aggregator.getWidth();

        if (hint > 0) {
            return range.roundToInterval(hint);
        } else {
            return range;
        }
    }

    private static final long DIFF = 3600 * 1000 * 6;

    public static interface StreamingQuery<T extends RangedQuery<T>> {
        public Callback<QueryMetricsResult> query(final T current, final AggregatorGroup aggregator);
    }

    private <T extends RangedQuery<T>> void stream(final T original,
            final T last, final AggregationGroup aggregation,
            final Stream.Handle<QueryMetricsResult> handle, final StreamingQuery<T> query) {
        final DateRange range = original.getRange();
        final DateRange currentRange = last.getRange();
        final DateRange nextRange = 
                currentRange.withStart(Math.max(currentRange.start() - DIFF, range.start()));

        final T current = last.withRange(nextRange);
        final AggregatorGroup aggregator = aggregation.build();

        query.query(current, aggregator).register(new Callback.Handle<QueryMetricsResult>() {
            @Override
            public void cancel(CancelReason reason) throws Exception {
                handle.close();
            }

            @Override
            public void error(Throwable e) throws Exception {
                handle.close();
            }

            @Override
            public void finish(QueryMetricsResult result) throws Exception {
                handle.stream(result);

                if (current.getRange().start() <= range.start()) {
                    handle.close();
                    return;
                }

                stream(original, current, aggregation, handle, query);
            }
        });
    }
}
