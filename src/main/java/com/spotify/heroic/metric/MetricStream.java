package com.spotify.heroic.metric;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metric.model.QueryMetricsResult;
import com.spotify.heroic.metric.model.StreamMetricsResult;

public interface MetricStream {
    public void stream(Callback<StreamMetricsResult> callback, QueryMetricsResult result) throws Exception;
}