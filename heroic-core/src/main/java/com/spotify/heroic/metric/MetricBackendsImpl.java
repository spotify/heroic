package com.spotify.heroic.metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregationcache.AggregationCache;
import com.spotify.heroic.aggregationcache.CacheOperationException;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.async.StreamReducer;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.async.AggregatedCallbackStream;
import com.spotify.heroic.metric.async.CacheGetTransformer;
import com.spotify.heroic.metric.async.SimpleCallbackStream;
import com.spotify.heroic.metric.exceptions.BackendOperationException;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.MetricBackendsReporter;

@Slf4j
@RequiredArgsConstructor
public class MetricBackendsImpl implements MetricBackends {
    private final MetricBackendsReporter reporter;
    private final AggregationCache cache;
    private final int disabled;
    private final List<MetricBackend> backends;

    public static interface BackendOp {
        void run(int disabled, MetricBackend backend) throws Exception;
    }

    private void execute(BackendOp op) {
        for (final MetricBackend b : backends) {
            try {
                op.run(disabled, b);
            } catch (final Exception e) {
                log.error("Backend operation failed", e);
            }
        }
    }

    @Override
    public List<Future<FetchData>> fetchAll(final Series series, final DateRange range) {
        final List<Future<FetchData>> callbacks = new ArrayList<>();

        execute(new BackendOp() {
            @Override
            public void run(int disabled, MetricBackend backend) throws Exception {
                callbacks.addAll(backend.fetch(series, range));
            }
        });

        return callbacks;
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
     * @param aggregation
     *            Aggregation method to use.
     * @return The result in the form of MetricGroups.
     * @throws BackendOperationException
     */
    @Override
    public Future<MetricGroups> query(final Map<String, String> group, final Filter filter, final Set<Series> series,
            final DateRange range, final AggregationGroup aggregation) {
        final List<Future<MetricGroups>> grouped = new ArrayList<>();

        execute(new BackendOp() {
            @Override
            public void run(final int disabled, final MetricBackend backend) throws Exception {
                // do not cache results if any backends are disabled or
                // unavailable,
                // because that would contribute to messed up results.
                final boolean noCache = disabled > 0;

                final Future<MetricGroups> fetch;

                if (cache.isConfigured() && !noCache) {
                    fetch = cachedFetch(backend, group, filter, series, range, aggregation);
                } else {
                    fetch = fetch(backend, group, series, range, aggregation);
                }

                grouped.add(fetch.transform(MetricGroups.identity(), MetricGroups.seriesError(group)));
            }
        });

        return Futures.reduce(grouped, MetricGroups.merger()).register(reporter.reportQuery());
    }

    /**
     * Perform a direct write on available configured backends.
     *
     * @param writes
     *            Batch of writes to perform.
     * @return A callback indicating how the writes went.
     * @throws BackendOperationException
     */
    @Override
    public Future<WriteBatchResult> write(final Collection<WriteMetric> writes) {
        final List<Future<WriteBatchResult>> callbacks = new ArrayList<>();

        execute(new BackendOp() {
            @Override
            public void run(int disabled, MetricBackend backend) throws Exception {
                callbacks.add(backend.write(writes));
            }
        });

        return Futures.reduce(callbacks, WriteBatchResult.merger()).register(reporter.reportWrite());
    }

    /**
     * Fetch data from cache merged with backends.
     */
    private Future<MetricGroups> cachedFetch(final MetricBackend backend, final Map<String, String> group,
            final Filter filter, final Set<Series> series, DateRange range, final AggregationGroup aggregation)
            throws CacheOperationException {
        final CacheGetTransformer transformer = new CacheGetTransformer(cache) {
            @Override
            public Future<MetricGroups> cacheMiss(Map<String, String> group, DateRange missedRange) throws Exception {
                return fetch(backend, group, series, missedRange, aggregation);
            }
        };

        return cache.get(filter, group, aggregation, range).transform(transformer);
    }

    private Future<MetricGroups> fetch(final MetricBackend backend, final Map<String, String> group,
            final Set<Series> series, final DateRange range, AggregationGroup aggregation) {
        final DateRange modified = aggregateRange(range, aggregation);
        final List<Future<FetchData>> fetches = new ArrayList<Future<FetchData>>();

        for (final Series serie : series)
            fetches.addAll(backend.fetch(serie, modified));

        return Futures.reduce(fetches, aggregationReducer(group, range, aggregation));
    }

    /**
     * Return a modified range if an aggregation is specified.
     *
     * Aggregations require a range to get modified according to their extent in order to 'fetch' datapoints for the
     * entire aggregation range.
     *
     * @param range
     *            The range to modify.
     * @param aggregation
     *            The aggregation to use for the modification.
     * @return The original range if no aggregation is specified, otherwise a modified one.
     */
    private DateRange aggregateRange(final DateRange range, final AggregationGroup aggregation) {
        if (aggregation == null)
            return range;

        return range.shiftStart(-aggregation.getSampling().getExtent());
    }

    private StreamReducer<FetchData, MetricGroups> aggregationReducer(Map<String, String> group, final DateRange range,
            final AggregationGroup aggregation) {
        if (aggregation == null)
            return new SimpleCallbackStream(group);

        return new AggregatedCallbackStream(group, aggregation.session(range));
    }
}
