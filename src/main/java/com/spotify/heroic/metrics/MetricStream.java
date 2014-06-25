package com.spotify.heroic.metrics;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.http.model.MetricsQueryResponse;
import com.spotify.heroic.metrics.model.StreamMetricsResult;

public interface MetricStream {
    public void stream(Callback<StreamMetricsResult> callback,
            MetricsQueryResponse result) throws Exception;
}