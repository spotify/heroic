package com.spotify.heroic.metrics;

import java.util.List;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.backend.Backend;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.metrics.kairosdb.DataPointsRowKey;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.FindRows;
import com.spotify.heroic.metrics.model.GetAllRowsResult;
import com.spotify.heroic.model.DateRange;

public interface MetricBackend extends Backend {
    /**
     * Query for data points that is part of the specified list of rows and
     * range.
     * 
     * @param fetchDataPointsQuery
     *            The query for fetching data points. The query contains rows
     *            and a specified time range.
     * 
     * @return A list of asynchronous data handlers for the resulting data
     *         points. This is suitable to use with GroupQuery. There will be
     *         one query per row.
     * 
     * @throws QueryException
     */
    public List<Callback<FetchDataPoints.Result>> query(
            FetchDataPoints fetchDataPointsQuery);

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
    public Callback<FindRows.Result> findRows(FindRows query)
            throws QueryException;

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
    public Callback<Long> getColumnCount(final DataPointsRowKey row,
            DateRange range);
}
