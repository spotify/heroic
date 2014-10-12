package com.spotify.heroic.metric;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.exceptions.BackendOperationException;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

public interface MetricBackends {
    public List<Future<FetchData>> fetchAll(final Series series, final DateRange range);

    /**
     * Perform a direct query on the configured backends.
     *
     * @param key
     *            Key of series to query.
     * @param series
     *            Set of series to query.
     * @param range
     *            Range of series to query.
     * @param aggregation
     *            Aggregation method to use.
     * @return The result in the form of MetricGroups.
     * @throws BackendOperationException
     */
    public Future<MetricGroups> query(final Map<String, String> group, final Filter filter, final Set<Series> series,
            final DateRange range, final AggregationGroup aggregation);

    /**
     * Perform a direct write on available configured backends.
     *
     * @param writes
     *            Batch of writes to perform.
     * @return A callback indicating how the writes went.
     * @throws BackendOperationException
     */
    public Future<WriteBatchResult> write(final Collection<WriteMetric> writes);
}
