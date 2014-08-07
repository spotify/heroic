package com.spotify.heroic.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.annotation.Nullable;
import javax.inject.Inject;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.http.model.MetricsQueryResponse;
import com.spotify.heroic.http.model.MetricsRequest;
import com.spotify.heroic.http.rpc.model.RpcQueryRequest;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.model.TimeSerieQuery;
import com.spotify.heroic.metrics.async.FindTimeSeriesTransformer;
import com.spotify.heroic.metrics.async.MergeWriteResponse;
import com.spotify.heroic.metrics.async.MetricGroupsTransformer;
import com.spotify.heroic.metrics.async.TimeSeriesTransformer;
import com.spotify.heroic.metrics.model.FindTimeSeriesCriteria;
import com.spotify.heroic.metrics.model.FindTimeSeriesGroups;
import com.spotify.heroic.metrics.model.GroupedTimeSeries;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.metrics.model.StreamMetricsResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.TimeSerie;
import com.spotify.heroic.model.WriteEntry;
import com.spotify.heroic.model.WriteResponse;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;

@RequiredArgsConstructor
@Slf4j
public class MetricBackendManager {
    private final MetricBackendManagerReporter reporter;
    private final List<MetricBackend> backends;
    private final long maxAggregationMagnitude;
    private final boolean updateMetadata;

    @Inject
    @Nullable
    private AggregationCache aggregationCache;

    @Inject
    private MetadataBackendManager metadata;

    @Inject
    private ClusterManager cluster;

    /**
     * Used for deferring work to avoid deep stack traces.
     */
    private final Executor deferredExecutor = Executors.newFixedThreadPool(10);

    interface BackendOperation {
        void run(int disabled, MetricBackend backend) throws Exception;
    }

    public Callback<WriteResponse> write(final Collection<WriteEntry> writes) {
        final List<Callback<WriteResponse>> callbacks = new ArrayList<Callback<WriteResponse>>();

        with(new BackendOperation() {
            @Override
            public void run(int disabled, MetricBackend backend) throws Exception {
                callbacks.add(backend.write(writes));
            }
        });

        // Send new time series to metadata backends.
        if (updateMetadata) {
            for (final WriteEntry entry : writes) {
                if (metadata.isReady())
                    callbacks.add(metadata.write(entry.getTimeSerie()));
            }
        }

        if (callbacks.isEmpty())
            return new CancelledCallback<WriteResponse>(
                    CancelReason.NO_BACKENDS_AVAILABLE);

        return ConcurrentCallback
                .newReduce(callbacks, MergeWriteResponse.get());
    }

    public Callback<MetricsQueryResponse> queryMetrics(
            final MetricsRequest query) throws MetricQueryException {
        if (query == null)
            throw new MetricQueryException("Query must be defined");

        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();
        final DateRange range = query.getRange().buildDateRange();

        final AggregationGroup aggregation = buildAggregationGroup(query);

        if (key == null || key.isEmpty())
            throw new MetricQueryException("'key' must be defined");

        if (range == null)
            throw new MetricQueryException("Range must be specified");

        if (!(range.start() < range.end()))
            throw new MetricQueryException(
                    "Range start must come before its end");

        if (aggregation != null) {
            final long memoryMagnitude = aggregation
                    .getCalculationMemoryMagnitude(range);

            if (memoryMagnitude > maxAggregationMagnitude) {
                throw new MetricQueryException(
                        "This query would result in too many datapoints");
            }
        }

        final DateRange rounded = roundRange(aggregation, range);

        final FindTimeSeriesCriteria criteria = new FindTimeSeriesCriteria(key, tags, tags, groupBy,
                rounded);

        final TimeSeriesTransformer transformer = new TimeSeriesTransformer(
                aggregationCache, aggregation, criteria.getRange());

        return findAndRouteTimeSeries(criteria).transform(transformer)
                .transform(new MetricGroupsTransformer(rounded))
                .register(reporter.reportQueryMetrics());
    }

    public Callback<StreamMetricsResult> streamMetrics(MetricsRequest query,
            MetricStream handle) throws MetricQueryException {
        final String key = query.getKey();
        final List<String> groupBy = query.getGroupBy();
        final Map<String, String> tags = query.getTags();

        if (key == null || key.isEmpty())
            throw new MetricQueryException("'key' must be defined");

        final AggregationGroup aggregation = buildAggregationGroup(query);

        final DateRange range = query.getRange().buildDateRange();

        if (aggregation != null) {
            final long memoryMagnitude = aggregation
                    .getCalculationMemoryMagnitude(range);

            if (memoryMagnitude > maxAggregationMagnitude) {
                throw new MetricQueryException(
                        "This query would result in too many datapoints");
            }
        }

        final DateRange rounded = roundRange(aggregation, range);

        final FindTimeSeriesCriteria criteria = new FindTimeSeriesCriteria(key, tags, tags, groupBy,
                rounded);

        final Callback<List<GroupedTimeSeries>> rows = findAndRouteTimeSeries(criteria);

        final Callback<StreamMetricsResult> callback = new ConcurrentCallback<StreamMetricsResult>();

        final String streamId = Integer.toHexString(criteria.hashCode());

        log.info("{}: streaming {}", streamId, criteria);

        final StreamingQuery streamingQuery = new StreamingQuery() {
            @Override
            public Callback<MetricGroups> query(DateRange range) {
                log.info("{}: streaming {}", streamId, range);
                return rows.transform(new TimeSeriesTransformer(
                        aggregationCache, aggregation, range));
            }
        };

        streamChunks(callback, handle, streamingQuery, criteria,
                rounded.start(rounded.end()), INITIAL_DIFF);

        return callback.register(reporter.reportStreamMetrics()).register(new Callback.Finishable() {
            @Override
            public void finished() throws Exception {
                log.info("{}: done streaming", streamId);
            }
        });
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
            final FindTimeSeriesCriteria original, final DateRange lastRange,
            final long window) {
        final DateRange originalRange = original.getRange();

        // decrease the range for the current chunk.
        final DateRange currentRange = lastRange.start(Math.max(
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
                } catch (final Exception e) {
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

                // ignore queries where parts of it is cached.
                if (cache.getHits() != 0) {
                    return window;
                }

                final long diff = System.currentTimeMillis() - then;

                if (diff >= QUERY_THRESHOLD) {
                    return window;
                }

                final double factor = ((Long) QUERY_THRESHOLD).doubleValue()
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

    /**
     * Check if the query wants to hint at a specific interval. If that is the
     * case, round the provided date to the specified interval.
     *
     * @param query
     * @return
     */
    private DateRange roundRange(AggregationGroup aggregation, DateRange range) {
        if (aggregation == null)
            return range;

        final Sampling sampling = aggregation.getSampling();
        return range.rounded(sampling.getExtent()).rounded(sampling.getSize())
                .shiftStart(-sampling.getExtent());
    }

    /**
     * Shorthand for running the operation on all available partitions.
     * @param op
     */
    private void with(BackendOperation op) {
        with(null, op);
    }

    /**
     * Function used to execute a backend operation on eligible backends.
     *
     * This will take care not to select disabled or unavailable backends.
     */
    private void with(final TimeSerie match, BackendOperation op) {
        final List<MetricBackend> alive = new ArrayList<MetricBackend>();

        // Keep track of disabled partitions.
        // This will have implications on;
        // 1) If the result if an operation can be cached or not.
        int disabled = 0;

        for (final MetricBackend backend : backends) {
            if (!backend.isReady()) {
                ++disabled;
                continue;
            }

            alive.add(backend);
        }

        for (final MetricBackend backend : alive) {
            try {
                op.run(disabled, backend);
            } catch (final Exception e) {
                log.error("Backend operation failed", e);
            }
        }
    }

    /**
     * Finds time series and routing the query to a specific remote Heroic
     * instance.
     * 
     * @param criteria
     * @return
     */
    private Callback<List<GroupedTimeSeries>> findAndRouteTimeSeries(
            final FindTimeSeriesCriteria criteria) {
        final Callback<List<GroupedTimeSeries>> query = findAllTimeSeries(criteria).transform(new Callback.Transformer<FindTimeSeriesGroups, List<GroupedTimeSeries>>() {
            @Override
            public List<GroupedTimeSeries> transform(final FindTimeSeriesGroups result) throws Exception {
                final List<GroupedTimeSeries> grouped = new ArrayList<GroupedTimeSeries>();

                for (final Entry<TimeSerie, Set<TimeSerie>> group : result.getGroups().entrySet()) {
                    final Set<TimeSerie> timeseries = group
                            .getValue();
                    if (timeseries.isEmpty()) {
                        continue;
                    }
                    final Set<Map<String, String>> tagsSet = new HashSet<>();
                                    NodeRegistryEntry node = null;
                    for (final TimeSerie timeSerie : timeseries) {
                                        node = cluster.findNode(timeSerie
                                                .getTags());
                                        tagsSet.add(node.getMetadata()
                                                .getTags());
                    }
                    if (tagsSet.size() != 1) {
                        throw new IllegalArgumentException(
                                "You are not allowed to perform global aggregate!");
                    }
                }

                return grouped;
            }
        });

        return query.register(reporter.reportFindTimeSeries());
    }

    public Callback<FindTimeSeriesGroups> findAllTimeSeries(final FindTimeSeriesCriteria query) {
        final TimeSerieQuery metaQuery = new TimeSerieQuery(query.getKey(), query.getFilter(), null);
        final FindTimeSeriesTransformer transformer = new FindTimeSeriesTransformer(query.getGroup(), query.getGroupBy());
        return metadata.findTimeSeries(metaQuery).transform(transformer);
    }

    private AggregationGroup buildAggregationGroup(final MetricsRequest query) {
        final List<Aggregation> aggregators = query.getAggregators();

        if (aggregators == null || aggregators.isEmpty())
            return null;

        return new AggregationGroup(aggregators, aggregators.get(0)
                .getSampling());
    }

    public Callback<MetricsQueryResponse> rpcQueryMetrics(RpcQueryRequest query) {
        final TimeSeriesTransformer transformer = new TimeSeriesTransformer(
                aggregationCache, query.getAggregationGroup(), query.getRange());

        return groupTimeseries(query.getKey(), query.getTimeseries())
                .transform(transformer)
                .transform(new MetricGroupsTransformer(query.getRange()))
                .register(reporter.reportQueryMetrics());
    }

    private Callback<List<GroupedTimeSeries>> groupTimeseries(
            final TimeSerie key,
            final Set<TimeSerie> timeseries) {
        final List<GroupedTimeSeries> grouped = new ArrayList<>();

        with(new BackendOperation() {
            @Override
            public void run(final int disabled, final MetricBackend backend)
                    throws Exception {
                // do not cache results if any backends are disabled or
                // unavailable,
                // because that would contribute to messed up results.
                final boolean noCache = disabled > 0;

                grouped.add(new GroupedTimeSeries(key, backend, timeseries,
                        noCache));
            }
        });
        return new ResolvedCallback<List<GroupedTimeSeries>>(grouped);
    }
}
