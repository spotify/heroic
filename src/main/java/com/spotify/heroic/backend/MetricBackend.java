package com.spotify.heroic.backend;

import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Getter;

import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.query.DateRange;

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

    public List<Query<DataPointsResult>> query(List<DataPointsRowKey> rows,
            DateRange range) throws QueryException;

    public static class FindRowsResult {
        @Getter
        private final List<DataPointsRowKey> rows;

        @Getter
        private final MetricBackend backend;

        public FindRowsResult(List<DataPointsRowKey> rows, MetricBackend backend) {
            this.rows = rows;
            this.backend = backend;
        }

        public boolean isEmpty() {
            return rows.isEmpty();
        }
    }

    public Query<FindRowsResult> findRows(String key, DateRange range,
            final Map<String, String> filter) throws QueryException;

    public static class FindTagsResult {
        @Getter
        private final Map<String, Set<String>> tags;

        @Getter
        private final List<String> metrics;

        public FindTagsResult(Map<String, Set<String>> tags,
                List<String> metrics) {
            this.tags = tags;
            this.metrics = metrics;
        }
    }

    public Query<FindTagsResult> findTags(Map<String, String> filter,
            Set<String> namesFilter) throws QueryException;
}
