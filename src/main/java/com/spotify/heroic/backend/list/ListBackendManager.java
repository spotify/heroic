package com.spotify.heroic.backend.list;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.inject.Inject;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.MetricGroupsTransformer;
import com.spotify.heroic.backend.MetricStream;
import com.spotify.heroic.backend.QueryException;
import com.spotify.heroic.backend.model.GroupedAllRowsResult;
import com.spotify.heroic.backend.model.MetricGroups;
import com.spotify.heroic.backend.model.Statistics;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.events.EventBackend;
import com.spotify.heroic.http.model.MetricsQueryResponse;
import com.spotify.heroic.http.model.MetricsRequest;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.model.FindRows;
import com.spotify.heroic.metrics.model.GetAllTimeSeries;
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
    private final BackendManagerReporter reporter;

    @Getter
    private final long maxAggregationMagnitude;

    @Inject
    private AggregationCache aggregationCache;

    /**
     * Used for deferring work to avoid deep stack traces.
     */
    private final Executor deferredExecutor = Executors.newFixedThreadPool(10);

    private <T> List<T> defaultList(List<T> list) {
        if (list == null)
            return new ArrayList<T>();

        return list;
    }

    @Override
    public Callback<MetricsQueryResponse> queryMetrics(final MetricsRequest query)
            throws QueryException {
        if (query == null)
            throw new QueryException("Query must be defined");

        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final DateRange range = query.getRange().buildDateRange();
        final AggregationGroup aggregation = new AggregationGroup(defaultList(query.getAggregators()));

        if (key == null || key.isEmpty())
            throw new QueryException("'key' must be defined");

        if (range == null)
            throw new QueryException("Range must be specified");

        if (!(range.start() < range.end()))
            throw new QueryException("Range start must come before its end");

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
                .transform(new RowGroupsTransformer(aggregationCache, aggregator, criteria.getRange()))
                .transform(new MetricGroupsTransformer(rounded))
                .register(reporter.reportQueryMetrics());
    }

    @Override
    public Callback<StreamMetricsResult> streamMetrics(MetricsRequest query, MetricStream handle)
            throws QueryException {
        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final List<Aggregation> definitions = defaultList(query.getAggregators());

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
                return rowGroups.transform(new RowGroupsTransformer(aggregationCache, aggregator, range));
            }
        };

        streamChunks(callback, handle, streamingQuery, criteria, rounded.withStart(rounded.end()), INITIAL_DIFF);

        return callback.register(reporter.reportStreamMetrics());
    }

    @Override
    public Callback<GroupedAllRowsResult> getAllRows() {
        final List<Callback<GetAllTimeSeries>> backendRequests = new ArrayList<Callback<GetAllTimeSeries>>();
        final Callback<GroupedAllRowsResult> all = new ConcurrentCallback<GroupedAllRowsResult>();

        for (final MetricBackend backend : metricBackends) {
            backendRequests.add(backend.getAllTimeSeries());
        }

        final Callback.Reducer<GetAllTimeSeries, GroupedAllRowsResult> resultReducer = new Callback.Reducer<GetAllTimeSeries, GroupedAllRowsResult>() {
            @Override
            public GroupedAllRowsResult resolved(
                    Collection<GetAllTimeSeries> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                final Set<TimeSerie> result = new HashSet<TimeSerie>();

                for (final GetAllTimeSeries backendResult : results) {
                    result.addAll(backendResult.getTimeSeries());
                }

                return new GroupedAllRowsResult(result);
            }
        };

        return all.reduce(backendRequests, resultReducer).register(reporter.reportGetAllRows());
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

    private static final long INITIAL_DIFF = 3600 * 1000 * 6;
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
                if (!callback.isInitialized())
                    return;

                try {
                    handle.stream(callback, new MetricsQueryResponse(originalRange, result));
                } catch(Exception e) {
                    callback.fail(e);
                    return;
                }

                if (currentRange.start() <= originalRange.start()) {
                    callback.resolve(new StreamMetricsResult());
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
                query.query(currentRange).register(callbackHandle).register(reporter.reportStreamMetricsChunk());
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

        return ConcurrentCallback.newReduce(queries, new FindRowGroupsReducer())
                .register(reporter.reportFindRowGroups());
    }
}
