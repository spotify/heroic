package com.spotify.heroic.backend;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.model.GroupedAllRowsResult;
import com.spotify.heroic.http.model.MetricsRequest;
import com.spotify.heroic.http.model.MetricsQueryResponse;

public interface BackendManager {
    public Callback<MetricsQueryResponse> queryMetrics(MetricsRequest query)
            throws QueryException;

    @ToString(of={})
    @RequiredArgsConstructor
    public static final class StreamMetricsResult {
    }

    public Callback<StreamMetricsResult> streamMetrics(MetricsRequest query, MetricStream handle)
            throws QueryException;

    public Callback<GroupedAllRowsResult> getAllRows();
}
