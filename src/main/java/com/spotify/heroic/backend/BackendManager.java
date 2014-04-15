package com.spotify.heroic.backend;

import java.util.List;
import java.util.Map;

import lombok.Getter;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.backend.model.GroupedAllRowsResult;
import com.spotify.heroic.query.MetricsQuery;

public interface BackendManager {
    public static final class DataPointGroup {
        @Getter
        private final Map<String, String> tags;

        @Getter
        private final List<DataPoint> datapoints;

        public DataPointGroup(Map<String, String> tags,
                List<DataPoint> datapoints) {
            this.tags = tags;
            this.datapoints = datapoints;
        }
    }

    public static final class QueryMetricsResult {
        @Getter
        private final List<DataPointGroup> groups;
        @Getter
        private final long sampleSize;
        @Getter
        private final long outOfBounds;
        @Getter
        private final RowStatistics rowStatistics;

        public QueryMetricsResult(List<DataPointGroup> groups, long sampleSize,
                long outOfBounds, final RowStatistics rowStatistics) {
            this.groups = groups;
            this.sampleSize = sampleSize;
            this.outOfBounds = outOfBounds;
            this.rowStatistics = rowStatistics;
        }
    }

    public Callback<QueryMetricsResult> queryMetrics(MetricsQuery query)
            throws QueryException;

    public Callback<GroupedAllRowsResult> getAllRows();
}
