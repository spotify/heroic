package com.spotify.heroic.cluster;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

public interface ClusterNode {
    public Callback<MetricGroups> query(final String backendGroup,
            final Filter filter, final Map<String, String> group,
            final AggregationGroup aggregation, final DateRange range,
            final Set<Series> series);

    public Callback<WriteBatchResult> write(final String backendGroup,
            Collection<WriteMetric> writes);
}
