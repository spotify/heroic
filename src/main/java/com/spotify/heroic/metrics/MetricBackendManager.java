package com.spotify.heroic.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;
import javax.inject.Inject;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.Reducers;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.http.model.MetricsQueryResponse;
import com.spotify.heroic.http.model.MetricsRequest;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.async.MetricGroupsTransformer;
import com.spotify.heroic.metrics.async.RowGroupsTransformer;
import com.spotify.heroic.metrics.model.FindTimeSeries;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.metrics.model.StreamMetricsResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;

@RequiredArgsConstructor
@Slf4j
public class MetricBackendManager {
    @Getter
    private final MetricBackendManagerReporter reporter;

    @Getter
    private final long maxAggregationMagnitude;

    @Inject
    @Nullable
    private AggregationCache aggregationCache;

    @Inject
    private Set<MetricBackend> metricBackends;

    @Inject
    private MetadataBackendManager metadata;

    /**
     * Used for deferring work to avoid deep stack traces.
     */
    private final Executor deferredExecutor = Executors.newFixedThreadPool(10);

    private <T> List<T> defaultList(List<T> list) {
        if (list == null)
            return new ArrayList<T>();

        return list;
    }

    public Callback<WriteResponse> write(final TimeSerie timeSerie,
            final List<DataPoint> datapoints) {
        final List<Callback<WriteResponse>> writes = new ArrayList<Callback<WriteResponse>>();

        for (final MetricBackend backend : metricBackends) {
            if (!backend.matches(timeSerie))
                continue;

            try {
                writes.add(backend.write(timeSerie, datapoints));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        if (metadata.isReady())
            writes.add(metadata.write(timeSerie));

        return ConcurrentCallback.newReduce(writes,
                new Callback.Reducer<WriteResponse, WriteResponse>() {

            @Override
            public WriteResponse resolved(
                    Collection<WriteResponse> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled)
                            throws Exception {
                return new WriteResponse();
            }
        });
    }

    public Callback<MetricsQueryResponse> queryMetrics(
            final MetricsRequest query) throws MetricQueryException {
        if (query == null)
            throw new MetricQueryException("Query must be defined");

        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final DateRange range = query.getRange().buildDateRange();
        final AggregationGroup aggregation = new AggregationGroup(
                defaultList(query.getAggregators()));

        if (key == null || key.isEmpty())
            throw new MetricQueryException("'key' must be defined");

        if (range == null)
            throw new MetricQueryException("Range must be specified");

        if (!(range.start() < range.end()))
            throw new MetricQueryException(
                    "Range start must come before its end");

        final long memoryMagnitude = aggregation
                .getCalculationMemoryMagnitude(range);

        if (memoryMagnitude > maxAggregationMagnitude) {
            throw new MetricQueryException(
                    "This query would result in too many datapoints");
        }

        final DateRange rounded = roundRange(aggregation, range);
        final FindTimeSeries criteria = new FindTimeSeries(key, tags, groupBy,
                rounded);

        final RowGroupsTransformer transformer = new RowGroupsTransformer(
                aggregationCache, aggregation,
                criteria.getRange());

        return findTimeSeries(criteria).transform(transformer)
                .transform(new MetricGroupsTransformer(rounded))
                .register(reporter.reportQueryMetrics());
    }

    public Callback<StreamMetricsResult> streamMetrics(MetricsRequest query,
            MetricStream handle) throws MetricQueryException {
        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final List<Aggregation> definitions = defaultList(query
                .getAggregators());

        if (key == null || key.isEmpty())
            throw new MetricQueryException("'key' must be defined");

        final AggregationGroup aggregation = new AggregationGroup(definitions);
        final DateRange range = query.getRange().buildDateRange();
        final DateRange rounded = roundRange(aggregation, range);

        final FindTimeSeries criteria = new FindTimeSeries(key, tags, groupBy,
                rounded);
        final Callback<List<FindTimeSeries.Result>> rows = findTimeSeries(criteria);

        final Callback<StreamMetricsResult> callback = new ConcurrentCallback<StreamMetricsResult>();

        final StreamingQuery streamingQuery = new StreamingQuery() {
            @Override
            public Callback<MetricGroups> query(DateRange range) {
                log.info("streaming {} on {}", criteria, range);
                return rows.transform(new RowGroupsTransformer(
                        aggregationCache, aggregation, range));
            }
        };

        streamChunks(callback, handle, streamingQuery, criteria,
                rounded.withStart(rounded.end()), INITIAL_DIFF);

        return callback.register(reporter.reportStreamMetrics());
    }

    public Callback<Set<TimeSerie>> getAllTimeSeries() {
        final List<Callback<Set<TimeSerie>>> backendRequests = new ArrayList<Callback<Set<TimeSerie>>>();

        for (final MetricBackend backend : metricBackends) {
            backendRequests.add(backend.getAllTimeSeries());
        }

        return ConcurrentCallback.newReduce(backendRequests,
                Reducers.<TimeSerie> joinSets()).register(
                        reporter.reportGetAllRows());
    }

    /**
     * Check if the query wants to hint at a specific interval. If that is the
     * case, round the provided date to the specified interval.
     * 
     * @param query
     * @return
     */
    private DateRange roundRange(AggregationGroup aggregator, DateRange range) {
        final long hint = aggregator.getWidth();

        if (hint > 0) {
            return range.roundToInterval(hint);
        } else {
            return range;
        }
    }

    private static final long INITIAL_DIFF = 3600 * 1000 * 6;
    private static final long QUERY_THRESHOLD = 10 * 1000;

    public static interface StreamingQuery {
        public Callback<MetricGroups> query(final DateRange range);
    }

    /**
     * Streaming implementation that backs down in time in DIFF ms for each
     * invocation.
     * 
     * @param callback
     * @param aggregation
     * @param handle
     * @param query
     * @param original
     * @param last
     */
    private void streamChunks(final Callback<StreamMetricsResult> callback,
            final MetricStream handle, final StreamingQuery query,
            final FindTimeSeries original, final DateRange lastRange,
            final long window) {
        final DateRange originalRange = original.getRange();

        // decrease the range for the current chunk.
        final DateRange currentRange = lastRange.withStart(Math.max(
                lastRange.start() - window, originalRange.start()));

        final long then = System.currentTimeMillis();

        final Callback.Handle<MetricGroups> callbackHandle = new Callback.Handle<MetricGroups>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                callback.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                callback.fail(e);
            }

            @Override
            public void resolved(MetricGroups result) throws Exception {
                // is cancelled?
                if (!callback.isReady())
                    return;

                try {
                    handle.stream(callback, new MetricsQueryResponse(
                            originalRange, result));
                } catch (Exception e) {
                    callback.fail(e);
                    return;
                }

                if (currentRange.start() <= originalRange.start()) {
                    callback.resolve(new StreamMetricsResult());
                    return;
                }

                final long nextWindow = calculateNextWindow(then, result,
                        window);
                streamChunks(callback, handle, query, original, currentRange,
                        nextWindow);
            }

            private long calculateNextWindow(long then, MetricGroups result,
                    long window) {
                final Statistics s = result.getStatistics();
                final Statistics.Cache cache = s.getCache();

                if (cache.getHits() != 0) {
                    return window;
                }

                final long diff = System.currentTimeMillis() - then;

                if (diff >= QUERY_THRESHOLD) {
                    return window;
                }

                double factor = ((Long) QUERY_THRESHOLD).doubleValue()
                        / ((Long) diff).doubleValue();
                return (long) (window * factor);
            }
        };

        /* Prevent long stack traces for very fast queries. */
        deferredExecutor.execute(new Runnable() {
            @Override
            public void run() {
                query.query(currentRange).register(callbackHandle)
                .register(reporter.reportStreamMetricsChunk());
            }
        });
    }

    private Callback<List<FindTimeSeries.Result>> findTimeSeries(
            FindTimeSeries criteria) {
        final List<Callback<FindTimeSeries.Result>> queries = new ArrayList<Callback<FindTimeSeries.Result>>();

        for (final MetricBackend backend : metricBackends) {
            try {
                queries.add(backend.findTimeSeries(criteria));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(queries,
                Reducers.<FindTimeSeries.Result> list()).register(
                        reporter.reportFindTimeSeries());
    }
}
