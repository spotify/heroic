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

    /**
     * Query for data points that is part of the specified list of rows and
     * range.
     * 
     * @param rows
     *            Query metrics for the specified rows.
     * @param range
     *            Filter on the specified date range.
     * @return A list of asynchronous data handlers for the resulting data
     *         points. This is suitable to use with GroupQuery. There will be
     *         one query per row.
     * 
     * @throws QueryException
     */
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

    /**
     * Find the data point rows matching the specified criteria.
     * 
     * @param key
     *            Only return rows matching this key.
     * @param range
     *            Filter on the specified date range.
     * @param filter
     *            Filter on the specified tags.
     * @return An asynchronous handler resulting in a FindRowsResult.
     * @throws QueryException
     */
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

    /**
     * Find the tags matching the specified query.
     * 
     * WARNING! This is slow since it's not optimized in the schema.
     * 
     * @param filter
     *            Only return tags matching the specified filter of key and
     *            value combinations.
     * @param namesFilter
     *            Only return results which has these specified tags present.
     * @return An asynchronous handler resulting in a FindTagsResult.
     * @throws QueryException
     *             If unable to setup the specified query.
     */
    public Query<FindTagsResult> findTags(Map<String, String> filter,
            Set<String> namesFilter) throws QueryException;
}
