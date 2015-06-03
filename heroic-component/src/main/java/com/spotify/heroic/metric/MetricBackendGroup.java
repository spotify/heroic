package com.spotify.heroic.metric;

import java.util.List;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.exceptions.BackendGroupException;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.ResultGroups;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;

import eu.toolchain.async.AsyncFuture;

public interface MetricBackendGroup extends MetricBackend {
    /**
     * Perform a direct query for data points.
     *
     * @param source
     *            TimeData source.
     * @param key
     *            Key of series to query.
     * @param series
     *            Set of series to query.
     * @param range
     *            Range of series to query.
     * @param aggregation
     *            Aggregation method to use.
     * @return The result in the form of MetricGroups.
     * @throws BackendGroupException
     */
    public <T extends TimeData> AsyncFuture<ResultGroups> query(Class<T> source, final Filter filter,
            final List<String> groupBy, final DateRange range, Aggregation aggregation, final boolean noCache);

    /**
     * Fetch metrics with a default (no-op) quota watcher. This method allows for the fetching of an indefinite amount
     * of metrics.
     *
     * @see #MetricBackend#fetch(Class, Series, DateRange, QuotaWatcher)
     */
    public <T extends TimeData> AsyncFuture<FetchData<T>> fetch(Class<T> type, Series series, DateRange range);
}