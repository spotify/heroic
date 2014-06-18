package com.spotify.heroic.backend;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.BackendManager.StreamMetricsResult;
import com.spotify.heroic.http.model.MetricsQueryResponse;

public interface MetricStream {
    public void stream(Callback<StreamMetricsResult> callback, MetricsQueryResponse result)
            throws Exception;
}