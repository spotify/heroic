package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import lombok.Getter;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.spotify.heroic.aggregator.Aggregator;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.list.GetAllRows;
import com.spotify.heroic.backend.list.QueryGroup;
import com.spotify.heroic.backend.list.QuerySingle;
import com.spotify.heroic.query.DateRange;
import com.spotify.heroic.query.MetricsQuery;

public class ListBackendManager implements BackendManager {
    @Getter
    private final List<MetricBackend> metricBackends;

    @Getter
    private final List<EventBackend> eventBackends;

    @Getter
    private final long maxAggregationMagnitude;

    private final GetAllRows getAllRows;
    private final QuerySingle querySingle;
    private final QueryGroup queryGroup;

    public ListBackendManager(List<Backend> backends, MetricRegistry registry,
            long maxAggregationMagnitude) {
        this.metricBackends = filterMetricBackends(backends);
        this.eventBackends = filterEventBackends(backends);
        this.maxAggregationMagnitude = maxAggregationMagnitude;

        final Timer getAllRowsTimer = registry.timer(MetricRegistry.name(
                "heroic", "get-all-rows"));
        this.getAllRows = new GetAllRows(metricBackends, getAllRowsTimer);

        final Timer queryMetricsSingle = registry.timer(MetricRegistry.name(
                "heroic", "query-metrics", "single"));
        this.querySingle = new QuerySingle(metricBackends, queryMetricsSingle);
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
        final List<Aggregator.Definition> definitions = query.getAggregators();
        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final DateRange range = query.getRange();
        final Date start = range.start();
        final Date end = range.end();

        if (!end.after(start)) {
            throw new QueryException("End time must come after start");
        }

        final AggregatorGroup aggregators = buildAggregators(definitions,
                start, end);

        final DateRange roundedRange = roundRange(aggregators, range);

        final Date roundedStart = roundedRange.start();
        final Date roundedEnd = roundedRange.end();

        if (groupBy != null && !groupBy.isEmpty()) {
            MetricBackend.FindRowGroups criteria = new MetricBackend.FindRowGroups(
                    key, roundedStart, roundedEnd, tags, groupBy);
            return queryGroup.execute(criteria, aggregators);
        }

        final MetricBackend.FindRows criteria = new MetricBackend.FindRows(key,
                roundedStart, roundedEnd, tags);
        return querySingle.execute(criteria, aggregators);
    }

    @Override
    public Callback<GetAllRowsResult> getAllRows() {
        return this.getAllRows.execute();
    }

    private AggregatorGroup buildAggregators(
            List<Aggregator.Definition> definitions, Date start, Date end)
            throws QueryException {
        final List<Aggregator> instances = new ArrayList<Aggregator>();

        if (definitions == null) {
            return new AggregatorGroup(instances);
        }

        for (final Aggregator.Definition definition : definitions) {
            instances.add(definition.build(start.getTime(), end.getTime()));
        }

        final AggregatorGroup aggregators = new AggregatorGroup(instances);

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
    private DateRange roundRange(AggregatorGroup aggregators,
            DateRange range) {
        final long hint = aggregators.getIntervalHint();

        if (hint > 0) {
            return range.roundToInterval(hint);
        } else {
            return range;
        }
    }
}
