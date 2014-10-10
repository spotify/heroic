package com.spotify.heroic.metric.async;

import java.util.Map;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.PreparedQuery;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
public class LocalQuery implements PreparedQuery {
    private final MetricManager metrics;
    private final String backendGroup;
    private final Filter filter;
    private final Map<String, String> group;
    private final Set<Series> series;

    @Override
    public Future<MetricGroups> query(final DateRange range, final AggregationGroup aggregation) throws Exception {
        return metrics.useGroup(backendGroup).groupedQuery(group, filter, series, range, aggregation);
    }
}