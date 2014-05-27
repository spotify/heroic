package com.spotify.heroic.backend;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.MetricRegistry;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregator.AggregatorGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.Stream;
import com.spotify.heroic.backend.kairosdb.DataPointsRowKey;
import com.spotify.heroic.backend.list.FindRowGroupsReducer;
import com.spotify.heroic.backend.list.RowGroups;
import com.spotify.heroic.backend.list.RowGroupsTransformer;
import com.spotify.heroic.backend.model.FindRows;
import com.spotify.heroic.backend.model.GetAllRowsResult;
import com.spotify.heroic.backend.model.GroupedAllRowsResult;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.http.model.MetricsQuery;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;

@Slf4j
public class ListBackendManager implements BackendManager {
    @Getter
    private final List<MetricBackend> metricBackends;

    @Getter
    private final List<EventBackend> eventBackends;

    @Getter
    private final long maxAggregationMagnitude;

    @Getter
    private final AggregationCache cache;

    public ListBackendManager(List<Backend> backends, MetricRegistry registry,
            long maxAggregationMagnitude, long maxQueriableDataPoints,
            AggregationCache cache) {
        this.metricBackends = filterMetricBackends(backends);
        this.eventBackends = filterEventBackends(backends);
        this.maxAggregationMagnitude = maxAggregationMagnitude;
        this.cache = cache;
    }

    private List<EventBackend> filterEventBackends(List<Backend> backends) {
        final List<EventBackend> eventBackends = new ArrayList<EventBackend>();

        for (final Backend backend : backends) {
            if (backend instanceof EventBackend)
                eventBackends.add((EventBackend) backend);
        }

        return eventBackends;
    }

    private List<MetricBackend> filterMetricBackends(List<Backend> backends) {
        final List<MetricBackend> metricBackends = new ArrayList<MetricBackend>();

        for (final Backend backend : backends) {
            if (backend instanceof MetricBackend)
                metricBackends.add((MetricBackend) backend);
        }

        return metricBackends;
    }

    private <T> List<T> optionalEmptyList(List<T> list) {
        if (list == null)
            return new ArrayList<T>();

        return list;
    }

    @Override
    public Callback<QueryMetricsResult> queryMetrics(final MetricsQuery query)
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

        return findRowGroups(criteria).transform(new RowGroupsTransformer(cache, aggregator, criteria.getRange()));
    }

    @Override
    public Callback<StreamMetricsResult> streamMetrics(MetricsQuery query, Stream.Handle<QueryMetricsResult, StreamMetricsResult> handle)
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
            public Callback<QueryMetricsResult> query(DateRange range) {
                final AggregatorGroup aggregator = aggregation.build();
                return rowGroups.transform(new RowGroupsTransformer(cache, aggregator, range));
            }
        };

        streamChunks(callback, handle, streamingQuery, criteria, rounded.withStart(rounded.end()));

        return callback;
    }

    @Override
    public Callback<GroupedAllRowsResult> getAllRows() {
        final List<Callback<GetAllRowsResult>> backendRequests = new ArrayList<Callback<GetAllRowsResult>>();
        final Callback<GroupedAllRowsResult> overallCallback = new ConcurrentCallback<GroupedAllRowsResult>();

        for (final MetricBackend backend : metricBackends) {
            backendRequests.add(backend.getAllRows());
        }

        final Callback.Reducer<GetAllRowsResult, GroupedAllRowsResult> resultReducer = new Callback.Reducer<GetAllRowsResult, GroupedAllRowsResult>() {
            @Override
            public GroupedAllRowsResult done(
                    Collection<GetAllRowsResult> results,
                    Collection<Throwable> errors,
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

        return overallCallback.reduce(backendRequests,
                resultReducer);
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

    public static interface StreamingQuery {
        public Callback<QueryMetricsResult> query(final DateRange range);
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
        final Stream.Handle<QueryMetricsResult, StreamMetricsResult> handle,
        final StreamingQuery query,
        final FindRows original,
        final DateRange lastRange
    )
    {
        final DateRange originalRange = original.getRange();
        // decrease the range for the current chunk.
        final DateRange currentRange = lastRange.withStart(
                Math.max(lastRange.start() - DIFF, originalRange.start()));

        query.query(currentRange).register(new Callback.Handle<QueryMetricsResult>() {
            @Override
            public void cancel(CancelReason reason) throws Exception {
                handle.close();
            }

            @Override
            public void error(Throwable e) throws Exception {
                handle.close();
            }

            @Override
            public void finish(QueryMetricsResult result) throws Exception {
                // is cancelled?
                if (!callback.isInitialized())
                    return;

                handle.stream(callback, result);

                if (currentRange.start() <= originalRange.start()) {
                    callback.finish(new StreamMetricsResult());
                    handle.close();
                    return;
                }

                streamChunks(callback, handle, query, original, currentRange);
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
