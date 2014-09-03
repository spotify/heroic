package com.spotify.heroic.cluster;

import java.util.List;
import java.util.Set;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.error.BackendOperationException;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
public class LocalClusterNode implements ClusterNode {
    private final MetricBackendManager metrics;

    @Override
    public Callback<MetricGroups> query(String backendGroup, Series key,
            Set<Series> series, DateRange range, AggregationGroup aggregation) {
        try {
            return metrics.useGroup(backendGroup).groupedQuery(key, series,
                    range, aggregation);
        } catch (final BackendOperationException e) {
            return new FailedCallback<>(e);
        }
    }

    @Override
    public Callback<WriteBatchResult> write(List<WriteMetric> writes) {
        try {
            return metrics.writeDirect(metrics.useDefaultGroup(), writes);
        } catch (final BackendOperationException e) {
            return new FailedCallback<>(e);
        }
    }
}
