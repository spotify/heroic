package com.spotify.heroic.metric;

import java.util.Collection;
import java.util.List;

import com.spotify.heroic.async.Future;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metric.model.BackendEntry;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

public interface MetricBackend extends LifeCycle {
    public String getGroup();

    /**
     * Execute a single write.
     *
     * @param write
     * @return
     */
    public Future<WriteBatchResult> write(WriteMetric write);

    /**
     * Write a collection of datapoints for a specific time series.
     *
     * @param series
     *            Time serie to write to.
     * @param data
     *            Datapoints to write.
     * @return A callback indicating if the write was successful or not.
     */
    public Future<WriteBatchResult> write(Collection<WriteMetric> writes);

    /**
     * Query for data points that is part of the specified list of rows and range.
     *
     * @param query
     *            The query for fetching data points. The query contains rows and a specified time range.
     *
     * @return A list of asynchronous data handlers for the resulting data points. This is suitable to use with
     *         GroupQuery. There will be one query per row.
     *
     * @throws QueryException
     */
    public List<Future<FetchData>> fetch(final Series series, final DateRange range);

    /**
     * Gets the total number of columns that are in the given rows
     *
     * @param rows
     * @return
     */
    public Future<Long> getColumnCount(final Series series, DateRange range);

    @Override
    public boolean isReady();

    /**
     * List all series directly from the database.
     *
     * This will be incredibly slow.
     *
     * @return An iterator over all found time series.
     */
    public Iterable<BackendEntry> listEntries();
}
