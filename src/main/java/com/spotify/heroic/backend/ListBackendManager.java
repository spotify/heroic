package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.backend.list.FindRowGroupsReducer;
import com.spotify.heroic.backend.list.RowGroups;
import com.spotify.heroic.backend.list.RowGroupsTransformer;
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.backend.model.GetAllRowsResult;
import com.spotify.heroic.backend.model.GroupedAllRowsResult;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.http.model.MetricsQuery;
import com.spotify.heroic.http.model.MetricsQueryResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.statistics.BackendManagerReporter;

@RequiredArgsConstructor
@Slf4j
public class ListBackendManager implements BackendManager {
    @Getter
    private final List<MetricBackend> metricBackends;

    @Getter
    private final List<EventBackend> eventBackends;

    @Getter
    private final AggregationCache cache;

    @Getter
    private final BackendManagerReporter reporter;

    @Getter
    private final long maxAggregationMagnitude;

    /**
     * Used for deferring work to avoid deep stack traces.
     */
    private final Executor deferredExecutor = Executors.newFixedThreadPool(10);

    private <T> List<T> optionalEmptyList(List<T> list) {
        if (list == null)
            return new ArrayList<T>();

        return list;
    }

    @Override
    public Callback<MetricsQueryResult> queryMetrics(final MetricsQuery query)
            throws QueryException {
        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final DateRange range = query.getRange().buildDateRange();
        final List<Aggregation> definitions = optionalEmptyList(query.getAggregators());

        if (key == null || key.isEmpty())
            throw new QueryException("'key' must be defined");

        if (range == null)
            throw new QueryException("Range must be specified");

        if (!(range.start() < range.end()))
            throw new QueryException("Range start must come before its end");

        final AggregationGroup aggregation = new AggregationGroup(definitions);
        final AggregatorGroup aggregator = aggregation.build();

        final long memoryMagnitude = aggregator
                .getCalculationMemoryMagnitude(range);

        if (memoryMagnitude > maxAggregationMagnitude) {
            throw new QueryException(
                    "This query would result in too many datapoints");
        }

        final DateRange rounded = roundRange(aggregator, range);
        final FindRows criteria = new FindRows(key, rounded, tags, groupBy);

        return findRowGroups(criteria)
                .transform(new RowGroupsTransformer(cache, aggregator, criteria.getRange()))
                .transform(new MetricGroupsTransformer(rounded))
                .register(reporter.timeQueryMetrics());
    }

    @Override
    public Callback<StreamMetricsResult> streamMetrics(MetricsQuery query, MetricStream handle)
            throws QueryException {
        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final List<Aggregation> definitions = optionalEmptyList(query.getAggregators());

        if (key == null || key.isEmpty())
            throw new QueryException("'key' must be defined");

        final AggregationGroup aggregation = new AggregationGroup(definitions);
        final AggregatorGroup aggregator = aggregation.build();

        final DateRange range = query.getRange().buildDateRange();
        final DateRange rounded = roundRange(aggregator, range);
        final FindRows criteria = new FindRows(key, rounded, tags, groupBy);
        final Callback<RowGroups> rowGroups = findRowGroups(criteria);

        final Callback<StreamMetricsResult> callback = new ConcurrentCallback<StreamMetricsResult>();

        final StreamingQuery streamingQuery = new StreamingQuery() {
            @Override
            public Callback<MetricGroups> query(DateRange range) {
                final AggregatorGroup aggregator = aggregation.build();
                return rowGroups.transform(new RowGroupsTransformer(cache, aggregator, range));
            }
        };

        streamChunks(callback, handle, streamingQuery, criteria, rounded.withStart(rounded.end()), DIFF);

        return callback.register(reporter.timeStreamMetrics());
    }

    @Override
    public Callback<GroupedAllRowsResult> getAllRows() {
        final List<Callback<GetAllRowsResult>> backendRequests = new ArrayList<Callback<GetAllRowsResult>>();
        final Callback<GroupedAllRowsResult> all = new ConcurrentCallback<GroupedAllRowsResult>();

        for (final MetricBackend backend : metricBackends) {
            backendRequests.add(backend.getAllRows());
        }

        final Callback.Reducer<GetAllRowsResult, GroupedAllRowsResult> resultReducer = new Callback.Reducer<GetAllRowsResult, GroupedAllRowsResult>() {
            @Override
            public GroupedAllRowsResult done(
                    Collection<GetAllRowsResult> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                final Set<TimeSerie> result = new HashSet<TimeSerie>();

                for (final GetAllRowsResult backendResult : results) {
                    final Map<String, List<DataPointsRowKey>> rows = backendResult
                            .getRows();

                    for (final Map.Entry<String, List<DataPointsRowKey>> entry : rows
                            .entrySet()) {
                        for (final DataPointsRowKey rowKey : entry.getValue()) {
                            result.add(new TimeSerie(rowKey.getMetricName(),
                                    rowKey.getTags()));
                        }
                    }
                }

                return new GroupedAllRowsResult(result);
            }
        };

        return all.reduce(backendRequests, resultReducer).register(reporter.timeGetAllRows());
    }

    /**
     * Check if the query wants to hint at a specific interval. If that is the
     * case, round the provided date to the specified interval.
     * 
     * @param query
     * @return
     */
    private DateRange roundRange(AggregatorGroup aggregator, DateRange range) {
        final long hint = aggregator.getWidth();

        if (hint > 0) {
            return range.roundToInterval(hint);
        } else {
            return range;
        }
    }

    private static final long DIFF = 3600 * 1000 * 6;
    private static final long QUERY_THRESHOLD = 10 * 1000;

    public static interface StreamingQuery {
        public Callback<MetricGroups> query(final DateRange range);
    }

    /**
     * Streaming implementation that backs down in time in DIFF ms for each invocation.
     *
     * @param callback
     * @param aggregation
     * @param handle
     * @param query
     * @param original
     * @param last
     */
    private void streamChunks(
        final Callback<StreamMetricsResult> callback,
        final MetricStream handle,
        final StreamingQuery query,
        final FindRows original,
        final DateRange lastRange,
        final long window
    )
    {

        final DateRange originalRange = original.getRange();
        // decrease the range for the current chunk.
        final DateRange currentRange = lastRange.withStart(
                Math.max(lastRange.start() - window, originalRange.start()));

    	log.info("Querying range {} with window {}", currentRange, window); 
        final long then = System.currentTimeMillis();

        final Callback.Handle<MetricGroups> callbackHandle = new Callback.Handle<MetricGroups>() {
            @Override
            public void cancel(CancelReason reason) throws Exception {
                callback.cancel(reason);
            }

            @Override
            public void error(Exception e) throws Exception {
                callback.fail(e);
            }

            @Override
            public void finish(MetricGroups result) throws Exception {
                // is cancelled?
                if (!callback.isInitialized())
                    return;

                try {
                    handle.stream(callback, new MetricsQueryResult(originalRange, result));
                } catch(Exception e) {
                    callback.fail(e);
                    return;
                }

                if (currentRange.start() <= originalRange.start()) {
                    callback.finish(new StreamMetricsResult());
                    return;
                }

                final long nextWindow = calculateNextWindow(then, result, window);
                streamChunks(callback, handle, query, original, currentRange, nextWindow);
            }

            private long calculateNextWindow(long then, MetricGroups result, long window) {
                final Statistics s = result.getStatistics();
                final Statistics.Cache cache = s.getCache();

                if (cache.getHits() != 0) {
                    return window;
                }

                final long diff = System.currentTimeMillis() - then;

                if (diff >= QUERY_THRESHOLD) {
                    return window;
                }

                double factor = ((Long)QUERY_THRESHOLD).doubleValue() / ((Long)diff).doubleValue();
                return (long) (window * factor);
            }
        };

        /* Prevent long stack traces for very fast queries. */
        deferredExecutor.execute(new Runnable() {
            @Override
            public void run() {
                query.query(currentRange).register(callbackHandle).register(reporter.timeStreamMetricsChunk());
            }
        });
    }

    private Callback<RowGroups> findRowGroups(FindRows criteria) {
        final List<Callback<FindRows.Result>> queries = new ArrayList<Callback<FindRows.Result>>();

        for (final MetricBackend backend : metricBackends) {
            try {
                queries.add(backend.findRows(criteria));
            } catch (final Exception e) {
                log.error("Failed to query backend", e);
            }
        }

        return ConcurrentCallback.newReduce(queries, new FindRowGroupsReducer());
    }
}
