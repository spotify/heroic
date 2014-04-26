package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Getter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.spotify.heroic.aggregator.Aggregation;
import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.backend.list.QueryGroup;
import com.spotify.heroic.backend.list.QuerySingle;
import com.spotify.heroic.backend.model.FindRowGroups;
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.backend.model.GetAllRowsResult;
import com.spotify.heroic.backend.model.GroupedAllRowsResult;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.MetricsQuery;

public class ListBackendManager implements BackendManager {
    @Getter
    private final List<MetricBackend> metricBackends;

    @Getter
    private final List<EventBackend> eventBackends;

    @Getter
    private final long maxAggregationMagnitude;

    @Getter
    private final AggregationCache cache;

    private final Timer getAllRowsTimer;

    private final QuerySingle querySingle;
    private final QueryGroup queryGroup;

    public ListBackendManager(List<Backend> backends, MetricRegistry registry,
            long maxAggregationMagnitude, long maxQueriableDataPoints,
            AggregationCache cache) {
        this.metricBackends = filterMetricBackends(backends);
        this.eventBackends = filterEventBackends(backends);
        this.maxAggregationMagnitude = maxAggregationMagnitude;
        this.cache = cache;

        getAllRowsTimer = registry.timer(MetricRegistry.name("heroic",
                "get-all-rows"));

        final Timer queryMetricsSingle = registry.timer(MetricRegistry.name(
                "heroic", "query-metrics", "single"));
        this.querySingle = new QuerySingle(metricBackends, queryMetricsSingle,
                maxQueriableDataPoints, cache);
        final Timer queryMetricsGroup = registry.timer(MetricRegistry.name(
                "heroic", "query-metrics", "group"));
        this.queryGroup = new QueryGroup(metricBackends, queryMetricsGroup);
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

    @Override
    public Callback<QueryMetricsResult> queryMetrics(final MetricsQuery query)
            throws QueryException {
        final List<Aggregation> definitions = query.getAggregators();
        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final DateRange range = query.getRange();

        if (range == null)
            throw new QueryException("Range must be specified");

        if (!(range.start() < range.end()))
            throw new QueryException("Range start must come before its end");

        final AggregatorGroup aggregators = buildAggregators(definitions, range);

        final DateRange rounded = roundRange(aggregators, range);

        if (groupBy != null && !groupBy.isEmpty()) {
            final FindRowGroups criteria = new FindRowGroups(key, rounded,
                    tags, groupBy);
            return queryGroup.execute(criteria, aggregators);
        }

        final FindRows criteria = new FindRows(key, rounded, tags);
        return querySingle.execute(criteria, aggregators);
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

        return overallCallback.reduce(backendRequests, getAllRowsTimer, resultReducer);
    }

    private AggregatorGroup buildAggregators(
            List<Aggregation> definitions, DateRange range)
            throws QueryException {
        final List<Aggregator> instances = new ArrayList<Aggregator>();

        if (definitions == null || definitions.isEmpty()) {
            return new AggregatorGroup(instances, null);
        }

        for (final Aggregation definition : definitions) {
            instances.add(definition.build(range));
        }

        final AggregatorGroup aggregators = new AggregatorGroup(instances,
                definitions.get(0));

        final long memoryMagnitude = aggregators
                .getCalculationMemoryMagnitude();

        if (memoryMagnitude > maxAggregationMagnitude) {
            throw new QueryException(
                    "This query would result in too many datapoints");
        }

        return aggregators;
    }

    /**
     * Check if the query wants to hint at a specific interval. If that is the
     * case, round the provided date to the specified interval.
     * 
     * @param query
     * @return
     */
    private DateRange roundRange(AggregatorGroup aggregators, DateRange range) {
        final long hint = aggregators.getIntervalHint();

        if (hint > 0) {
            return range.roundToInterval(hint);
        } else {
            return range;
        }
    }
}
