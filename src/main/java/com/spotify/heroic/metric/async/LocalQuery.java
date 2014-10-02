package com.spotify.heroic.metric.async;

import java.util.Map;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.MetricBackendManager;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.PreparedQuery;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
public class LocalQuery implements PreparedQuery {
    private final MetricBackendManager metrics;
    private final String backendGroup;
    private final Filter filter;
    private final Map<String, String> group;
    private final Set<Series> series;

    @Override
    public Callback<MetricGroups> query(final DateRange range, final AggregationGroup aggregation) throws Exception {
        return metrics.useGroup(backendGroup).groupedQuery(group, filter, series, range, aggregation);
    }
}