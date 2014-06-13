package com.spotify.heroic.backend;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.BackendManager.MetricGroups;
import com.spotify.heroic.http.model.MetricsQueryResult;
import com.spotify.heroic.model.DateRange;

@RequiredArgsConstructor
public class MetricGroupsTransformer implements Callback.Transformer<MetricGroups, MetricsQueryResult> {
    private final DateRange rounded;

    @Override
    public void transform(MetricGroups result, Callback<MetricsQueryResult> callback) throws Exception {
        callback.finish(new MetricsQueryResult(rounded, result));
    }
}