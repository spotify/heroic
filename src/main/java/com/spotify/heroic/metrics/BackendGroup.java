package com.spotify.heroic.metrics;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.metrics.async.MergeWriteResult;
import com.spotify.heroic.metrics.async.TimeSeriesTransformer;
import com.spotify.heroic.metrics.error.BackendOperationException;
import com.spotify.heroic.metrics.model.GroupedTimeSeries;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.FetchData;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteResult;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;

@Slf4j
@RequiredArgsConstructor
public class BackendGroup {
    private final AggregationCache cache;
    private final MetricBackendManagerReporter reporter;
    private final int disabled;
    private final List<Backend> backends;

    private void execute(BackendOperation op) {
        for (final Backend b : backends) {
            try {
                op.run(disabled, b);
            } catch (final Exception e) {
                log.error("Backend operation failed", e);
            }
        }
    }

    /**
     * Perform a direct query on the configured backends.
     *
     * @param key
     *            Key of series to query.
     * @param series
     *            Set of series to query.
     * @param range
     *            Range of series to query.
     * @param aggregationGroup
     *            Aggregation method to use.
     * @return The result in the form of MetricGroups.
     * @throws BackendOperationException
     */
    public Callback<MetricGroups> groupedQuery(final Series key,
            final Set<Series> series, final DateRange range,
            final AggregationGroup aggregationGroup) {
        final TimeSeriesTransformer transformer = new TimeSeriesTransformer(
                cache, aggregationGroup, range);

        return groupTimeseries(key, series).transform(transformer).register(
                reporter.reportRpcQueryMetrics());
    }

    public List<Callback<FetchData>> query(final Series series,
            final DateRange range) {
        final List<Callback<FetchData>> callbacks = new ArrayList<>();

        execute(new BackendOperation() {
            @Override
            public void run(int disabled, Backend backend) throws Exception {
                callbacks.addAll(backend.fetch(series, range));
            }
        });

        return callbacks;
    }

    private Callback<List<GroupedTimeSeries>> groupTimeseries(final Series key,
            final Set<Series> timeseries) {
        final List<GroupedTimeSeries> grouped = new ArrayList<>();

        execute(new BackendOperation() {
            @Override
            public void run(final int disabled, final Backend backend)
                    throws Exception {
                // do not cache results if any backends are disabled or
                // unavailable,
                // because that would contribute to messed up results.
                final boolean noCache = disabled > 0;

                grouped.add(new GroupedTimeSeries(key, backend, timeseries,
                        noCache));
            }
        });

        return new ResolvedCallback<>(grouped);
    }

    /**
     * Perform a direct write on available configured backends.
     *
     * @param writes
     *            Batch of writes to perform.
     * @return A callback indicating how the writes went.
     * @throws BackendOperationException
     */
    public Callback<WriteResult> write(final List<WriteMetric> writes) {
        final List<Callback<WriteResult>> callbacks = new ArrayList<Callback<WriteResult>>();

        execute(new BackendOperation() {
            @Override
            public void run(int disabled, Backend backend) throws Exception {
                callbacks.add(backend.write(writes));
            }
        });

        return ConcurrentCallback.newReduce(callbacks, MergeWriteResult.get());
    }
}
