package com.spotify.heroic.backend;

import java.util.List;

import lombok.Getter;

import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.query.MetricsQuery;

public interface MetricBackend extends Backend {
    public static class DataPointsResult {
        @Getter
        private final List<DataPoint> datapoints;

        @Getter
        private final DataPointsRowKey rowKey;

        public DataPointsResult(List<DataPoint> datapoints,
                DataPointsRowKey rowKey) {
            this.datapoints = datapoints;
            this.rowKey = rowKey;
        }
    }

    public static interface DataPointsQuery {
        public GroupQuery<DataPointsResult> query(MetricsQuery query)
                throws QueryException;

        /**
         * Check if this engine is empty.
         * 
         * I.e. It will never give any results at all.
         * 
         * @return boolean indicating if this engine is empty or not.
         */
        public boolean isEmpty();
    }

    public Query<DataPointsQuery> query(MetricsQuery query)
            throws QueryException;
}
