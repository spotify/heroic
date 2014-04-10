package com.spotify.heroic.backend;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.ToString;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.kairosdb.DataPoint;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;

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
    public List<Callback<DataPointsResult>> query(List<DataPointsRowKey> rows,
            Date start, Date end) throws QueryException;

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

    public static class FindRows {
        @Getter
        private final String key;
        @Getter
        private final Date start;
        @Getter
        private final Date end;
        @Getter
        private final Map<String, String> filter;

        public FindRows(String key, Date start, Date end,
                Map<String, String> filter) {
            this.key = key;
            this.start = start;
            this.end = end;
            this.filter = filter;
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
    public Callback<FindRowsResult> findRows(FindRows query);

    public static class FindRowGroupsResult {
        @Getter
        private final Map<Map<String, String>, List<DataPointsRowKey>> rowGroups;

        @Getter
        private final MetricBackend backend;

        public FindRowGroupsResult(
                Map<Map<String, String>, List<DataPointsRowKey>> rowGroups,
                MetricBackend backend) {
            this.rowGroups = rowGroups;
            this.backend = backend;
        }
    }

    @ToString(of = { "key", "filter", "groupBy" })
    public static class FindRowGroups {
        @Getter
        private final String key;
        @Getter
        private final Date start;
        @Getter
        private final Date end;
        @Getter
        private final Map<String, String> filter;
        @Getter
        private final List<String> groupBy;

        public FindRowGroups(String key, Date start, Date end,
                Map<String, String> filter, List<String> groupBy) {
            this.key = key;
            this.start = start;
            this.end = end;
            this.filter = filter;
            this.groupBy = groupBy;
        }
    }

    /**
     * Find a rows by a group specification.
     * 
     * @param key
     *            Only return rows matching this key.
     * @param range
     *            Filter on the specified date range.
     * @param filter
     *            Filter on the specified tags.
     * @param groupBy
     *            Tags to group by, the order specified will result in the way
     *            the groups are returned.
     * @return An asynchronous handler resulting in a FindRowsResult.
     * @throws QueryException
     */
    public Callback<FindRowGroupsResult> findRowGroups(FindRowGroups query)
            throws QueryException;

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

    @ToString(of = { "rows" })
    public static class GetAllRowsResult {
        @Getter
        private final Map<String, List<DataPointsRowKey>> rows;

        public GetAllRowsResult(Map<String, List<DataPointsRowKey>> rows) {
            this.rows = rows;
        }
    }

    /**
     * Gets all available rows
     * 
     * @return An asynchronous handler resulting in a {@link GetAllRowsResult}
     */
    public Callback<GetAllRowsResult> getAllRows();

    /**
     * Gets the total number of columns that are in the given rows
     * 
     * @param rows
     * @return
     */
    public List<Callback<Long>> getColumnCount(List<DataPointsRowKey> rows,
            DateRange range);
}
