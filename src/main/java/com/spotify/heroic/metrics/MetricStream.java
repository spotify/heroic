package com.spotify.heroic.metrics;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metrics.model.QueryMetricsResult;
import com.spotify.heroic.metrics.model.StreamMetricsResult;

public interface MetricStream {
    public void stream(Callback<StreamMetricsResult> callback,
            QueryMetricsResult result) throws Exception;
}