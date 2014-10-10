package com.spotify.heroic.metric.model;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.model.DateRange;

public interface PreparedQuery {
    public Future<MetricGroups> query(final DateRange range, final AggregationGroup aggregationGroup)
            throws Exception;
}