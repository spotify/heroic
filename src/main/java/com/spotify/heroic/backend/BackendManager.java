package com.spotify.heroic.backend;

import java.util.Set;

import javax.ws.rs.container.AsyncResponse;

import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.query.MetricsQuery;
import com.spotify.heroic.query.TimeSeriesQuery;

public interface BackendManager {
    public void queryMetrics(MetricsQuery query, AsyncResponse response);

    public void queryTags(TimeSeriesQuery query, AsyncResponse response);

    @ToString(of = { "timeSeries" })
    public static class GetAllTimeSeriesResult {
        @Getter
        private final Set<TimeSerie> timeSeries;

        public GetAllTimeSeriesResult(Set<TimeSerie> timeSeries) {
            this.timeSeries = timeSeries;
        }
    }

    public Callback<GetAllTimeSeriesResult> getAllRows();
}
