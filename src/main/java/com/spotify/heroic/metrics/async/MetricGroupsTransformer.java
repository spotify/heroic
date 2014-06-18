package com.spotify.heroic.metrics.async;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.http.model.MetricsQueryResponse;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public class MetricGroupsTransformer implements Callback.DeferredTransformer<MetricGroups, MetricsQueryResponse> {
    private final DateRange rounded;

    @Override
    public void transform(MetricGroups result, Callback<MetricsQueryResponse> callback) throws Exception {
        callback.resolve(new MetricsQueryResponse(rounded, result));
    }
}