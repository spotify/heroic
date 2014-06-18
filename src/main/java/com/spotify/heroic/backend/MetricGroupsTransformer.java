package com.spotify.heroic.backend;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.model.MetricGroups;
import com.spotify.heroic.http.model.MetricsQueryResponse;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public class MetricGroupsTransformer implements Callback.Transformer<MetricGroups, MetricsQueryResponse> {
    private final DateRange rounded;

    @Override
    public void transform(MetricGroups result, Callback<MetricsQueryResponse> callback) throws Exception {
        callback.resolve(new MetricsQueryResponse(rounded, result));
    }
}