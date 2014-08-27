package com.spotify.heroic.cluster;

import java.util.List;
import java.util.Set;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteMetric;

public interface ClusterNode {
    public Callback<MetricGroups> query(final Series key,
            final Set<Series> series, final DateRange range,
            final AggregationGroup aggregationGroup);

    public Callback<Boolean> write(List<WriteMetric> writes);
}
