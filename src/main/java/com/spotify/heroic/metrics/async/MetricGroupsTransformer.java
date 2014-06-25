package com.spotify.heroic.metrics.async;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.http.model.MetricsQueryResponse;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public class MetricGroupsTransformer implements
        Callback.Transformer<MetricGroups, MetricsQueryResponse> {
    private final DateRange rounded;

    @Override
    public MetricsQueryResponse transform(MetricGroups result) throws Exception {
        return new MetricsQueryResponse(rounded, result);
    }
}