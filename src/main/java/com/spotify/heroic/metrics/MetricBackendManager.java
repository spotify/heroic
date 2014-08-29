package com.spotify.heroic.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;
import javax.inject.Inject;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.injection.Lifecycle;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.async.FindAndRouteTransformer;
import com.spotify.heroic.metrics.async.FindTimeSeriesTransformer;
import com.spotify.heroic.metrics.async.MergeWriteResult;
import com.spotify.heroic.metrics.async.MetricGroupsTransformer;
import com.spotify.heroic.metrics.async.PreparedQueryTransformer;
import com.spotify.heroic.metrics.async.TimeSeriesTransformer;
import com.spotify.heroic.metrics.model.FindTimeSeriesCriteria;
import com.spotify.heroic.metrics.model.FindTimeSeriesGroups;
import com.spotify.heroic.metrics.model.GroupedTimeSeries;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.PreparedQuery;
import com.spotify.heroic.metrics.model.QueryMetricsResult;
import com.spotify.heroic.metrics.model.StreamMetricsResult;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteMetric;
import com.spotify.heroic.model.WriteResult;
import com.spotify.heroic.model.filter.Filter;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;

@Slf4j
@RequiredArgsConstructor
@ToString(exclude = { "scheduledExecutor" })
public class MetricBackendManager implements Lifecycle {
    private final MetricBackendManagerReporter reporter;
    private final List<Backend> backends;
    private final boolean updateMetadata;
    private final int groupLimit;
    private final int groupLoadLimit;
    private final long flushingInterval = 10;

    private final BulkProcessor<WriteMetric> writeBulkProcessor = new BulkProcessor<>(
            new BulkProcessor.Flushable<WriteMetric>() {
                @Override
                public void flushWrites(List<WriteMetric> writes)
                        throws Exception {
                    log.info("Flushing {} write(s)", writes.size());
                    MetricBackendManager.this.flushWrites(writes);
                }
            });

    private final Callback.Transformer<WriteResult, Boolean> DIRECT_WRITE_TO_BOOLEAN = new Callback.Transformer<WriteResult, Boolean>() {
        @Override
        public Boolean transform(WriteResult result) throws Exception {
            for (final Exception e : result.getFailed()) {
                log.error("Write failed", e);
            }

            for (final CancelReason reason : result.getCancelled()) {
                log.error("Write cancelled: " + reason);
            }

            return result.getFailed().isEmpty()
                    && result.getCancelled().isEmpty();
        }
    };

    @Inject
    @Nullable
    private AggregationCache aggregationCache;

    @Inject
    private ClusterManager cluster;

    @Inject
    private MetadataBackendManager metadata;

    @Inject
    private ScheduledExecutorService scheduledExecutor;

    /**
     * Used for deferring work to avoid deep stack traces.
     */
    private final Executor deferredExecutor = Executors.newFixedThreadPool(10);

    public void flushWrites(List<WriteMetric> writes) throws Exception {
        final WriteBatchResult result;

        try {
            result = routeWrites(writes).get();
        } catch (final Exception e) {
            log.error("Write batch failed", e);
            throw new Exception("Write batch failed", e);
        }

        if (!result.isOk())
            throw new Exception("Write batch failed (asynchronously)");
    }

    public Backend getBackend(String id) {
        if (id == null)
            return null;

        for (final Backend b : backends) {
            if (b.getId().equals(id)) {
                return b;
            }
        }

        return null;
    }

    public List<Backend> getBackends() {
        return ImmutableList.copyOf(backends);
    }

    /**
     * Buffer a write to this backend, will block if the buffer is full.
     *
     * @param write
     *            The write to buffer.
     * @throws InterruptedException
     *             If the write was interrupted.
     * @throws BufferEnqueueException
     *             If the provided metric could not be buffered.
     * @throws MetricFormatException
     *             If the provided metric is invalid.
     */
    public void bufferWrite(WriteMetric write) throws InterruptedException,
            BufferEnqueueException, MetricFormatException {
        if (cluster != ClusterManager.NULL) {
            // TODO: don't do it like this.
            findNodeRegistryEntry(write);
        }

        writeBulkProcessor.enqueue(write);
    }

    /**
     * Perform a write that could be routed to other cluster nodes.
     *
     * @param writes
     *            Writes to perform.
     * @return A callback that will be fired when the write is done or failed.
     */
    private Callback<WriteBatchResult> routeWrites(
            final List<WriteMetric> writes) {
        final List<Callback<Boolean>> callbacks = new ArrayList<>();

        if (cluster == ClusterManager.NULL) {
            callbacks.add(writeDirect(writes)
                    .transform(DIRECT_WRITE_TO_BOOLEAN));
        } else {
            callbacks.addAll(writeCluster(writes));
        }

        final Callback.Reducer<Boolean, WriteBatchResult> reducer = new Callback.Reducer<Boolean, WriteBatchResult>() {
            @Override
            public WriteBatchResult resolved(Collection<Boolean> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                for (final Exception e : errors) {
                    log.error("Remote write failed", e);
                }

                for (final CancelReason reason : cancelled) {
                    log.error("Remote write cancelled: " + reason.getMessage());
                }

                Boolean ok = null;

                if (!errors.isEmpty() || !cancelled.isEmpty())
                    ok = false;

                for (final Boolean b : results) {
                    if (ok == null) {
                        ok = b;
                    } else {
                        ok &= b;
                    }
                }

                return new WriteBatchResult(ok, results.size() + errors.size()
                        + cancelled.size());
            }
        };

        return ConcurrentCallback.newReduce(callbacks, reducer);
    }

    private List<Callback<Boolean>> writeCluster(final List<WriteMetric> writes) {
        final List<Callback<Boolean>> callbacks = new ArrayList<>();

        final Map<NodeRegistryEntry, List<WriteMetric>> partitions = new HashMap<>();

        for (final WriteMetric write : writes) {
            final NodeRegistryEntry node;

            try {
                node = findNodeRegistryEntry(write);
            } catch (final MetricFormatException e) {
                log.warn("Write failed: {}", write, e);
                continue;
            }

            List<WriteMetric> partition = partitions.get(node);

            if (partition == null) {
                partition = new ArrayList<WriteMetric>();
                partitions.put(node, partition);
            }

            partition.add(write);
        }

        for (final Map.Entry<NodeRegistryEntry, List<WriteMetric>> entry : partitions
                .entrySet()) {
            final NodeRegistryEntry node = entry.getKey();
            final List<WriteMetric> nodeWrites = entry.getValue();

            // if request is local.
            if (node.getMetadata().getId().equals(cluster.getLocalNodeId())) {
                callbacks.add(writeDirect(nodeWrites).transform(
                        DIRECT_WRITE_TO_BOOLEAN));
            } else {
                callbacks.add(node.getClusterNode().write(nodeWrites));
            }
        }

        return callbacks;
    }

    private NodeRegistryEntry findNodeRegistryEntry(final WriteMetric write)
            throws MetricFormatException {
        final NodeRegistryEntry node = cluster.findNode(write.getSeries()
                .getTags(), NodeCapability.WRITE);

        if (node == null)
            throw new MetricFormatException("Cannot route " + write.getSeries()
                    + " to any known, writable node");

        return node;
    }

    public Callback<QueryMetricsResult> queryMetrics(final Filter filter,
            final List<String> groupBy, final DateRange range,
            final AggregationGroup aggregation) throws MetricQueryException {
        final DateRange rounded = roundRange(aggregation, range);

        final FindTimeSeriesCriteria criteria = new FindTimeSeriesCriteria(
                filter, groupBy, rounded);

        final PreparedQueryTransformer transformer = new PreparedQueryTransformer(
                cluster, rounded, aggregation);

        return findAndRouteTimeSeries(criteria).transform(transformer)
                .transform(new MetricGroupsTransformer(rounded))
                .register(reporter.reportQueryMetrics());
    }

    public Callback<StreamMetricsResult> streamMetrics(final Filter filter,
            final List<String> groupBy, final DateRange range,
            final AggregationGroup aggregation, MetricStream handle)
                    throws MetricQueryException {
        final DateRange rounded = roundRange(aggregation, range);

        final FindTimeSeriesCriteria criteria = new FindTimeSeriesCriteria(
                filter, groupBy, rounded);

        final Callback<List<PreparedQuery>> rows = findAndRouteTimeSeries(criteria);

        final Callback<StreamMetricsResult> callback = new ConcurrentCallback<StreamMetricsResult>();

        final String streamId = Integer.toHexString(criteria.hashCode());

        log.info("{}: streaming {}", streamId, criteria);

        final StreamingQuery streamingQuery = new StreamingQuery() {
            @Override
            public Callback<MetricGroups> query(DateRange range) {
                log.info("{}: streaming chunk {}", streamId, range);

                final PreparedQueryTransformer transformer = new PreparedQueryTransformer(
                        cluster, range, aggregation);

                return rows.transform(transformer);
            }
        };

        final long chunk = rounded.diff() / RANGE_FACTOR;

        streamChunks(callback, handle, streamingQuery, criteria,
                rounded.start(rounded.end()), chunk, chunk);

        return callback.register(reporter.reportStreamMetrics()).register(
                new Callback.Finishable() {
                    @Override
                    public void finished() throws Exception {
                        log.info("{}: done streaming", streamId);
                    }
                });
    }

    private static final long RANGE_FACTOR = 20;

    public static interface StreamingQuery {
        public Callback<MetricGroups> query(final DateRange range);
    }

    /**
     * Streaming implementation that backs down in time in DIFF ms for each
     * invocation.
     */
    private void streamChunks(final Callback<StreamMetricsResult> callback,
            final MetricStream handle, final StreamingQuery query,
            final FindTimeSeriesCriteria original, final DateRange lastRange,
            final long chunk, final long window) {
        final DateRange originalRange = original.getRange();

        // decrease the range for the current chunk.
        final DateRange currentRange = lastRange.start(Math.max(
                lastRange.start() - window, originalRange.start()));

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
                    handle.stream(callback, new QueryMetricsResult(
                            originalRange, result));
                } catch (final Exception e) {
                    callback.fail(e);
                    return;
                }

                if (currentRange.start() <= originalRange.start()) {
                    callback.resolve(new StreamMetricsResult());
                    return;
                }

                streamChunks(callback, handle, query, original, currentRange,
                        chunk, window + chunk);
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
     *
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
    private void with(final Series match, BackendOperation op) {
        final List<Backend> alive = new ArrayList<Backend>();

        // Keep track of disabled partitions.
        // This will have implications on;
        // 1) If the result if an operation can be cached or not.
        int disabled = 0;

        for (final Backend backend : backends) {
            if (!backend.isReady()) {
                ++disabled;
                continue;
            }

            alive.add(backend);
        }

        for (final Backend backend : alive) {
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
    private Callback<List<PreparedQuery>> findAndRouteTimeSeries(
            final FindTimeSeriesCriteria criteria) {
        return findAllTimeSeries(criteria).transform(
                new FindAndRouteTransformer(groupLimit, groupLoadLimit,
                        cluster, this)).register(
                                reporter.reportFindTimeSeries());
    }

    private Callback<FindTimeSeriesGroups> findAllTimeSeries(
            final FindTimeSeriesCriteria query) {
        final FindTimeSeriesTransformer transformer = new FindTimeSeriesTransformer(
                query.getGroupBy());
        return metadata.findTimeSeries(query.getFilter())
                .transform(transformer);
    }

    /**
     * Direct methods, mostly used for RPC.
     */

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
     */
    public Callback<MetricGroups> directQuery(final Series key,
            final Set<Series> series, final DateRange range,
            final AggregationGroup aggregationGroup) {
        final TimeSeriesTransformer transformer = new TimeSeriesTransformer(
                aggregationCache, aggregationGroup, range);

        return groupTimeseries(key, series).transform(transformer).register(
                reporter.reportRpcQueryMetrics());
    }

    private Callback<List<GroupedTimeSeries>> groupTimeseries(final Series key,
            final Set<Series> timeseries) {
        final List<GroupedTimeSeries> grouped = new ArrayList<>();

        with(new BackendOperation() {
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

        return new ResolvedCallback<List<GroupedTimeSeries>>(grouped);
    }

    /**
     * Perform a direct write on available configured backends.
     *
     * @param writes
     *            Batch of writes to perform.
     * @return A callback indicating how the writes went.
     */
    public Callback<WriteResult> writeDirect(final List<WriteMetric> writes) {
        final List<Callback<WriteResult>> callbacks = new ArrayList<Callback<WriteResult>>();

        with(new BackendOperation() {
            @Override
            public void run(int disabled, Backend backend) throws Exception {
                callbacks.add(backend.write(writes));
            }
        });

        // Send new time series to metadata backends.
        if (updateMetadata) {
            for (final WriteMetric entry : writes) {
                if (metadata.isReady())
                    callbacks.add(metadata.write(entry.getSeries()));
            }
        }

        if (callbacks.isEmpty())
            return new CancelledCallback<WriteResult>(
                    CancelReason.NO_BACKENDS_AVAILABLE);

        return ConcurrentCallback.newReduce(callbacks, MergeWriteResult.get());
    }

    public void scheduleFlush() {
        if (writeBulkProcessor.isStopped())
            return;

        scheduledExecutor.schedule(new Runnable() {
            @Override
            public void run() {
                if (writeBulkProcessor.isStopped())
                    return;

                writeBulkProcessor.flush();
                scheduleFlush();
            }
        }, flushingInterval, TimeUnit.SECONDS);
    }

    @Override
    public void start() throws Exception {
        scheduleFlush();
    }

    @Override
    public void stop() throws Exception {
        writeBulkProcessor.stop();
    }

    @Override
    public boolean isReady() {
        return false;
    }
}
