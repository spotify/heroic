package com.spotify.heroic.backend;

import java.util.List;
import java.util.Map;

import javax.ws.rs.container.AsyncResponse;

import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.query.MetricsQuery;
import com.spotify.heroic.query.TagsQuery;

public interface BackendManager {
    public void queryMetrics(MetricsQuery query, AsyncResponse response);

    public void queryTags(TagsQuery query, AsyncResponse response);

    @ToString(of = { "rows" })
    public static class GetAllRowsResult {
        @Getter
        private final Map<String, List<DataPointsRowKey>> rows;

        public GetAllRowsResult(Map<String, List<DataPointsRowKey>> rows) {
            this.rows = rows;
        }
    }

    public Callback<GetAllRowsResult> getAllRows();
}
