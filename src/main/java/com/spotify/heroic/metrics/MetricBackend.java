package com.spotify.heroic.metrics;

import java.util.List;
import java.util.Set;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.metrics.model.FetchDataPoints;
import com.spotify.heroic.metrics.model.FindTimeSeries;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.yaml.ValidationException;

public interface MetricBackend {
    public static interface YAML {
        MetricBackend build(String context, MetricBackendReporter reporter)
                throws ValidationException;
    }

    /**
     * Indicate if the specified time series matches this backend.
     * 
     * @param timeSerie
     * @return Return <code>true</code> if the specified time series is expected
     *         to be found in this backend. Else <code>false</code>
     */
    public boolean matches(final TimeSerie timeSerie);

    /**
     * Write a collection of datapoints for a specific time series.
     * 
     * @param timeSerie
     *            Time serie to write to.
     * @param datapoints
     *            Datapoints to write.
     * @return A callback indicating if the write was successful or not.
     */
    public Callback<WriteResponse> write(final TimeSerie timeSerie,
            final List<DataPoint> datapoints);

    /**
     * Query for data points that is part of the specified list of rows and
     * range.
     * 
     * @param query
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
            final TimeSerie timeSerie, final DateRange range);

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
    public Callback<FindTimeSeries.Result> findTimeSeries(FindTimeSeries query)
            throws MetricQueryException;

    /**
     * Gets all available rows
     * 
     * @return An asynchronous handler resulting in a {@link GetAllTimeSeries}
     */
    public Callback<Set<TimeSerie>> getAllTimeSeries();

    /**
     * Gets the total number of columns that are in the given rows
     * 
     * @param rows
     * @return
     */
    public Callback<Long> getColumnCount(final TimeSerie timeSerie,
            DateRange range);
}
