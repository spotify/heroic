package com.spotify.heroic.backend;

import javax.ws.rs.container.AsyncResponse;

import com.spotify.heroic.query.MetricsQuery;
import com.spotify.heroic.query.TagsQuery;

public interface BackendManager {
    public void queryMetrics(MetricsQuery query, AsyncResponse response);

    public void queryTags(TagsQuery query, AsyncResponse response);
}
