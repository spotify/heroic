package com.spotify.heroic.backend;

import javax.ws.rs.container.AsyncResponse;

import com.spotify.heroic.query.MetricsQuery;

public interface BackendManager {
    public void queryMetrics(MetricsQuery query, AsyncResponse response);
}
