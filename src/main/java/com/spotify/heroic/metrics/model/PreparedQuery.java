package com.spotify.heroic.metrics.model;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.model.DateRange;

public interface PreparedQuery {
    public Callback<MetricGroups> query(final DateRange range,
            final AggregationGroup aggregationGroup) throws Exception;
}