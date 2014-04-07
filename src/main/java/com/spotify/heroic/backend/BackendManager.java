package com.spotify.heroic.backend;

import java.util.List;
import java.util.Set;

import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.query.MetricsQuery;

public interface BackendManager {
    public static final class QueryMetricsResult {
        @Getter
        private final List<DataPoint> datapoints;
        @Getter
        private final long sampleSize;
        @Getter
        private final long outOfBounds;
        @Getter
        private final RowStatistics rowStatistics;

        public QueryMetricsResult(List<DataPoint> datapoints, long sampleSize,
                long outOfBounds, final RowStatistics rowStatistics) {
            this.datapoints = datapoints;
            this.sampleSize = sampleSize;
            this.outOfBounds = outOfBounds;
            this.rowStatistics = rowStatistics;
        }
    }

    public Callback<QueryMetricsResult> queryMetrics(MetricsQuery query)
            throws QueryException;

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
