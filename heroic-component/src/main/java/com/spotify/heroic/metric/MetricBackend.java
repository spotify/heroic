package com.spotify.heroic.metric;

import java.util.Collection;
import java.util.List;

import com.spotify.heroic.metric.model.BackendEntry;
import com.spotify.heroic.metric.model.BackendKey;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;
import com.spotify.heroic.utils.Grouped;
import com.spotify.heroic.utils.Initializing;

import eu.toolchain.async.AsyncFuture;

public interface MetricBackend extends Initializing, Grouped {
    /**
     * Execute a single write.
     *
     * @param write
     * @return
     * @throws MetricBackendException
     *             If the write cannot be performed.
     */
    public AsyncFuture<WriteResult> write(WriteMetric write);

    /**
     * Write a collection of datapoints for a specific time series.
     *
     * @param series
     *            Time serie to write to.
     * @param data
     *            Datapoints to write.
     * @return A callback indicating if the write was successful or not.
     * @throws MetricBackendException
     *             If the write cannot be performed.
     */
    public AsyncFuture<WriteResult> write(Collection<WriteMetric> writes);

    /**
     * Query for data points that is part of the specified list of rows and range.
     *
     * @param type
     *            The type of metric to fetch.
     * @param series
     *            The series to fetch metrics for.
     * @param range
     *            The range to fetch metrics for.
     * @param watcher
     *            The watcher implementation to use when fetching metrics.
     *
     * @return A future containing the fetched data wrapped in a {@link FetchData} structure.
     */
    public <T extends TimeData> AsyncFuture<FetchData<T>> fetch(Class<T> type, Series series, DateRange range,
            FetchQuotaWatcher watcher);

    /**
     * List all series directly from the database.
     *
     * This will be incredibly slow.
     *
     * @return An iterator over all found time series.
     * @throws MetricBackendException
     *             If listing of entries cannot be performed.
     */
    public Iterable<BackendEntry> listEntries();

    /**
     * Return a list of all matching backend keys.
     *
     * @param start
     *            If specified, limit start to this point.
     * @param end
     *            If specified, limit end to this point.
     * @param limit
     *            Limit the amount of results, max will always be 1000.
     * @return A future containing a list of backend keys.
     */
    public AsyncFuture<List<BackendKey>> keys(BackendKey start, BackendKey end, int limit);
}
