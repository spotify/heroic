package com.spotify.heroic.metric.async;

import java.util.Map;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.PreparedQuery;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
public class ClusterQuery implements PreparedQuery {
    private final String backendGroup;
    private final ClusterNode node;
    private final Filter filter;
    private final Map<String, String> group;
    private final Set<Series> series;

    @Override
    public Callback<MetricGroups> query(final DateRange range,
            final AggregationGroup aggregation) {
        return node.query(backendGroup, filter, group, aggregation, range,
                series);
    }
};