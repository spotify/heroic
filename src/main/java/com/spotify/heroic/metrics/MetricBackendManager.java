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

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.injection.Lifecycle;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.MetadataOperationException;
import com.spotify.heroic.metrics.async.FindAndRouteTransformer;
import com.spotify.heroic.metrics.async.FindTimeSeriesTransformer;
import com.spotify.heroic.metrics.async.MergeWriteResult;
import com.spotify.heroic.metrics.async.MetricGroupsTransformer;
import com.spotify.heroic.metrics.async.PreparedQueryTransformer;
import com.spotify.heroic.metrics.error.BackendOperationException;
import com.spotify.heroic.metrics.model.BufferedWriteMetric;
import com.spotify.heroic.metrics.model.FindTimeSeriesCriteria;
import com.spotify.heroic.metrics.model.FindTimeSeriesGroups;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.PreparedQuery;
import com.spotify.heroic.metrics.model.QueryMetricsResult;
import com.spotify.heroic.metrics.model.StreamMetricsResult;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.WriteResult;
import com.spotify.heroic.model.filter.Filter;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
@RequiredArgsConstructor
@ToString(exclude = { "scheduledExecutor" })
public class MetricBackendManager implements Lifecycle {
    public static final boolean DEFAULT_UPDATE_METADATA = false;
    public static final int DEFAULT_GROUP_LIMIT = 500;
    public static final int DEFAULT_GROUP_LOAD_LIMIT = 5000;
    public static final long DEFAULT_FLUSHING_INTERVAL = 10;

    @Data
    public static final class YAML {
        public List<Backend.YAML> backends = new ArrayList<>();
        private List<String> defaultBackends = null;
        private boolean updateMetadata = DEFAULT_UPDATE_METADATA;
        private int groupLimit = DEFAULT_GROUP_LIMIT;
        private int groupLoadLimit = DEFAULT_GROUP_LOAD_LIMIT;
        private long flushingInterval = DEFAULT_FLUSHING_INTERVAL;

        public MetricBackendManager build(ConfigContext ctx,
                MetricBackendManagerReporter reporter)
                throws ValidationException {
            final Map<String, List<Backend>> backends = buildBackends(ctx,
                    reporter);
            final List<Backend> defaultBackends = buildDefaultBackends(ctx,
                    backends);

            return new MetricBackendManager(reporter, backends,
                    defaultBackends, updateMetadata, groupLimit,
                    groupLoadLimit, flushingInterval);
        }

        private List<Backend> buildDefaultBackends(ConfigContext ctx,
                Map<String, List<Backend>> backends) throws ValidationException {
            if (defaultBackends == null) {
                final List<Backend> result = new ArrayList<>();

                for (final Map.Entry<String, List<Backend>> entry : backends
                        .entrySet()) {
                    result.addAll(entry.getValue());
                }

                return result;
            }

            final List<Backend> result = new ArrayList<>();

            for (final ConfigContext.Entry<String> entry : ctx.iterate(
                    defaultBackends, "defaultBackends")) {
                final List<Backend> someResult = backends.get(entry.getValue());

                if (someResult == null)
                    throw new ValidationException(entry.getContext(),
                            "No backend(s) available with id : "
                                    + entry.getValue());

                result.addAll(someResult);
            }

            return result;
        }

        private Map<String, List<Backend>> buildBackends(ConfigContext ctx,
                MetricBackendManagerReporter reporter)
                        throws ValidationException {
            final Map<String, List<Backend>> groups = new HashMap<>();

            for (final ConfigContext.Entry<Backend.YAML> c : ctx.iterate(
                    this.backends, "backends")) {
                final Backend backend = c.getValue().build(c.getContext(),
                        c.getIndex(),
                        reporter.newBackend(c.getContext().toString()));

                List<Backend> group = groups.get(backend.getGroup());

                if (group == null) {
                    group = new ArrayList<>();
                    groups.put(backend.getGroup(), group);
                }

                group.add(backend);
            }

            return groups;
        }
    }

    private final MetricBackendManagerReporter reporter;
    private final Map<String, List<Backend>> backends;
    private final List<Backend> defaultBackends;
    private final boolean updateMetadata;
    private final int groupLimit;
    private final int groupLoadLimit;
    private final long flushingInterval;

    private final BulkProcessor<BufferedWriteMetric> writeBulkProcessor = new BulkProcessor<>(
            new BulkProcessor.Flushable<BufferedWriteMetric>() {
                @Override
                public void flushWrites(List<BufferedWriteMetric> writes)
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
    private AggregationCache cache;

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

    public void flushWrites(List<BufferedWriteMetric> bufferedWrites)
            throws Exception {
        final Map<String, List<WriteMetric>> writes = groupByBackendGroup(bufferedWrites);

        final List<Callback<WriteBatchResult>> callbacks = new ArrayList<>();

        for (final Map.Entry<String, List<WriteMetric>> entry : writes
                .entrySet()) {
            callbacks.add(routeWrites(entry.getKey(), entry.getValue()));
        }

        final Callback.Reducer<WriteBatchResult, WriteBatchResult> reducer = new Callback.Reducer<WriteBatchResult, WriteBatchResult>() {
            @Override
            public WriteBatchResult resolved(
                    Collection<WriteBatchResult> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                boolean allOk = true;
                int requests = 0;

                for (final WriteBatchResult r : results) {
                    allOk = allOk && r.isOk();
                    requests += r.getRequests();
                }

                return new WriteBatchResult(allOk && errors.isEmpty()
                        && cancelled.isEmpty(), requests);
            }
        };

        final Callback<WriteBatchResult> callback = ConcurrentCallback
                .newReduce(callbacks, reducer);

        final WriteBatchResult result;

        try {
            result = callback.get();
        } catch (final Exception e) {
            throw new Exception("Write batch failed", e);
        }

        if (!result.isOk())
            throw new Exception("Write batch failed (asynchronously)");
    }

    private Map<String, List<WriteMetric>> groupByBackendGroup(
            List<BufferedWriteMetric> writes) {
        final Map<String, List<WriteMetric>> groups = new HashMap<>();

        for (final BufferedWriteMetric w : writes) {
            List<WriteMetric> group = groups.get(w.getBackendGroup());

            if (group == null) {
                group = new ArrayList<>();
                groups.put(w.getBackendGroup(), group);
            }

            group.add(new WriteMetric(w.getSeries(), w.getData()));
        }

        return groups;
    }

    public List<Backend> findBackends(Set<String> groups) {
        final List<Backend> result = new ArrayList<>();

        for (final String group : groups) {
            final List<Backend> partial = findBackends(group);

            if (partial == null)
                continue;

            result.addAll(partial);
        }

        if (result.isEmpty())
            return null;

        return ImmutableList.copyOf(result);
    }

    private static final List<Backend> EMPTY_RESULT = ImmutableList
            .copyOf(new ArrayList<Backend>());

    public List<Backend> findBackends(String group) {
        if (group == null)
            return null;

        final List<Backend> result = backends.get(group);

        if (result == null || result.isEmpty())
            return null;

        return ImmutableList.copyOf(result);
    }

    public Backend findOneBackend(String group) {
        final List<Backend> result = findBackends(group);

        if (result.isEmpty())
            return null;

        if (result.size() > 1)
            throw new IllegalStateException("Too many backends matching '"
                    + group + "' are available: " + result);

        return result.get(0);
    }

    public List<Backend> getBackends() {
        final List<Backend> result = new ArrayList<>();

        for (final Map.Entry<String, List<Backend>> entry : backends.entrySet())
            result.addAll(entry.getValue());

        return ImmutableList.copyOf(result);
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
    public void bufferWrite(String backendGroup, WriteMetric write)
            throws InterruptedException, BufferEnqueueException,
            MetricFormatException {
        if (cluster != ClusterManager.NULL) {
            // TODO: don't do it like this, but it's good if we pre-emptively
            // check that it _can_ be routed.
            findNodeRegistryEntry(write);
        }

        writeBulkProcessor.enqueue(new BufferedWriteMetric(backendGroup, write
                .getSeries(), write.getData()));
    }

    /**
     * Perform a write that could be routed to other cluster nodes.
     *
     * @param writes
     *            Writes to perform.
     * @return A callback that will be fired when the write is done or failed.
     * @throws BackendOperationException
     */
    private Callback<WriteBatchResult> routeWrites(final String backendGroup,
            final List<WriteMetric> writes) throws BackendOperationException {
        final List<Callback<Boolean>> callbacks = new ArrayList<>();

        if (cluster == ClusterManager.NULL) {
            final BackendGroup backend = useGroup(backendGroup);
            callbacks.add(writeDirect(backend, writes).transform(
                    DIRECT_WRITE_TO_BOOLEAN));
        } else {
            callbacks.addAll(writeCluster(backendGroup, writes));
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

                boolean ok = errors.isEmpty() && cancelled.isEmpty();

                if (ok) {
                    for (final boolean b : results) {
                        ok = ok && b;
                    }
                }

                return new WriteBatchResult(ok, results.size() + errors.size()
                        + cancelled.size());
            }
        };

        return ConcurrentCallback.newReduce(callbacks, reducer);
    }

    private List<Callback<Boolean>> writeCluster(final String backendGroup,
            final List<WriteMetric> writes) throws BackendOperationException {

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
                final BackendGroup backend = useGroup(backendGroup);
                callbacks.add(writeDirect(backend, nodeWrites).transform(
                        DIRECT_WRITE_TO_BOOLEAN));
            } else {
                callbacks.add(node.getClusterNode().write(nodeWrites));
            }
        }

        return callbacks;
    }

    private Callback<WriteResult> writeDirect(BackendGroup backend,
            List<WriteMetric> writes) {
        final List<Callback<WriteResult>> callbacks = new ArrayList<>();

        callbacks.add(backend.write(writes));

        // Send new time series to metadata backends.
        if (updateMetadata) {
            for (final WriteMetric entry : writes) {
                if (metadata.isReady()) {
                    try {
                        metadata.write(entry.getSeries());
                    } catch (final MetadataOperationException e) {
                        log.error("Failed to write metadata", e);
                    }
                }
            }
        }

        if (callbacks.isEmpty())
            return new CancelledCallback<WriteResult>(
                    CancelReason.NO_BACKENDS_AVAILABLE);

        return ConcurrentCallback.newReduce(callbacks, MergeWriteResult.get());
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

    public Callback<QueryMetricsResult> queryMetrics(final String backendGroup,
            final Filter filter, final List<String> groupBy,
            final DateRange range, final AggregationGroup aggregation)
            throws MetricQueryException {
        final DateRange rounded = roundRange(aggregation, range);

        final FindTimeSeriesCriteria criteria = new FindTimeSeriesCriteria(
                filter, groupBy, rounded);

        final PreparedQueryTransformer transformer = new PreparedQueryTransformer(
                cluster, rounded, aggregation);

        return findAndRouteTimeSeries(backendGroup, criteria)
                .transform(transformer)
                .transform(new MetricGroupsTransformer(rounded))
                .register(reporter.reportQueryMetrics());
    }

    public Callback<StreamMetricsResult> streamMetrics(
            final String backendGroup, final Filter filter,
            final List<String> groupBy, final DateRange range,
            final AggregationGroup aggregation, MetricStream handle)
            throws MetricQueryException {
        final DateRange rounded = roundRange(aggregation, range);

        final FindTimeSeriesCriteria criteria = new FindTimeSeriesCriteria(
                filter, groupBy, rounded);

        final Callback<List<PreparedQuery>> rows = findAndRouteTimeSeries(
                backendGroup, criteria);

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

    public BackendGroup useDefaultGroup() throws BackendOperationException {
        return useGroup(null);
    }

    public BackendGroup useGroup(final String group)
            throws BackendOperationException {
        final List<Backend> selected;

        if (group == null) {
            if (defaultBackends == null)
                throw new BackendOperationException(
                        "No default backend configured");

            selected = defaultBackends;
        } else {
            selected = findBackends(group);
        }

        if (selected == null || selected.isEmpty())
            throw new BackendOperationException("No backend(s) found");

        return useAlive(selected);
    }

    public BackendGroup useGroups(final Set<String> groups)
            throws BackendOperationException {
        final List<Backend> selected;

        if (groups == null) {
            if (defaultBackends == null)
                throw new BackendOperationException(
                        "No default backend configured");

            selected = defaultBackends;
        } else {
            selected = findBackends(groups);
        }

        if (selected == null || selected.isEmpty())
            throw new BackendOperationException("No backend(s) found");

        return useAlive(selected);
    }

    private BackendGroup useAlive(List<Backend> backends)
            throws BackendOperationException {
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

        if (alive.isEmpty())
            throw new BackendOperationException("No backend(s) available");

        return new BackendGroup(cache, reporter, disabled, alive);
    }

    /**
     * Finds time series and routing the query to a specific remote Heroic
     * instance.
     *
     * @param criteria
     * @return
     */
    private Callback<List<PreparedQuery>> findAndRouteTimeSeries(
            final String backendGroup, final FindTimeSeriesCriteria criteria) {
        return findAllTimeSeries(criteria).transform(
                new FindAndRouteTransformer(backendGroup, groupLimit,
                        groupLoadLimit, cluster, this)).register(
                reporter.reportFindTimeSeries());
    }

    private Callback<FindTimeSeriesGroups> findAllTimeSeries(
            final FindTimeSeriesCriteria query) {
        final FindTimeSeriesTransformer transformer = new FindTimeSeriesTransformer(
                query.getGroupBy());
        return metadata.findSeries(query.getFilter()).transform(transformer);
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
