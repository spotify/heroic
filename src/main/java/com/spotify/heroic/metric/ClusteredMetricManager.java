package com.spotify.heroic.metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.NodeCapability;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metadata.ClusteredMetadataManager;
import com.spotify.heroic.metric.async.FindSeriesTransformer;
import com.spotify.heroic.metric.async.MetricGroupsTransformer;
import com.spotify.heroic.metric.error.BackendOperationException;
import com.spotify.heroic.metric.error.BufferEnqueueException;
import com.spotify.heroic.metric.model.BufferedWriteMetric;
import com.spotify.heroic.metric.model.FindTimeSeriesGroups;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.QueryMetricsResult;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;

@Slf4j
@NoArgsConstructor
@ToString(exclude = { "scheduledExecutor" })
public class ClusteredMetricManager implements LifeCycle {
    @Inject
    @Named("groupLimit")
    private int groupLimit;

    @Inject
    @Named("groupLoadLimit")
    private int groupLoadLimit;

    @Inject
    @Named("flushingInterval")
    private long flushingInterval;

    @Inject
    private MetricBackendManagerReporter reporter;

    @Inject
    private ClusterManager cluster;

    @Inject
    private ClusteredMetadataManager metadata;

    @Inject
    private ScheduledExecutorService scheduledExecutor;

    private final MetricBulkProcessor<BufferedWriteMetric> writeBulkProcessor = new MetricBulkProcessor<>(
            new MetricBulkProcessor.Flushable<BufferedWriteMetric>() {
                @Override
                public void flushWrites(List<BufferedWriteMetric> writes) throws Exception {
                    log.info("Flushing {} write(s)", writes.size());
                    ClusteredMetricManager.this.flushWrites(writes);
                }
            });

    /**
     * Used for deferring work to avoid deep stack traces.
     */
    private final Executor deferredExecutor = Executors.newFixedThreadPool(10);

    public void flushWrites(List<BufferedWriteMetric> bufferedWrites) throws Exception {
        final Map<String, List<BufferedWriteMetric>> writes = groupByBackendGroup(bufferedWrites);

        final List<Callback<WriteBatchResult>> callbacks = new ArrayList<>();

        for (final Map.Entry<String, List<BufferedWriteMetric>> entry : writes.entrySet()) {
            callbacks.add(routeWrites(entry.getKey(), entry.getValue()));
        }

        final Callback.Reducer<WriteBatchResult, WriteBatchResult> reducer = new Callback.Reducer<WriteBatchResult, WriteBatchResult>() {
            @Override
            public WriteBatchResult resolved(Collection<WriteBatchResult> results, Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                for (final Exception e : errors)
                    log.error("Write failed", e);

                for (final CancelReason cancel : cancelled)
                    log.error("Write cancelled: {}", cancel);

                boolean allOk = true;
                int requests = 0;

                for (final WriteBatchResult r : results) {
                    allOk = allOk && r.isOk();
                    requests += r.getRequests();
                }

                return new WriteBatchResult(allOk && errors.isEmpty() && cancelled.isEmpty(), requests);
            }
        };

        final Callback<WriteBatchResult> callback = ConcurrentCallback.newReduce(callbacks, reducer);

        final WriteBatchResult result;

        try {
            result = callback.get();
        } catch (final Exception e) {
            throw new Exception("Write batch failed", e);
        }

        if (!result.isOk())
            throw new Exception("Write batch failed (asynchronously)");
    }

    private Map<String, List<BufferedWriteMetric>> groupByBackendGroup(List<BufferedWriteMetric> writes) {
        final Map<String, List<BufferedWriteMetric>> groups = new HashMap<>();

        for (final BufferedWriteMetric w : writes) {
            List<BufferedWriteMetric> group = groups.get(w.getBackendGroup());

            if (group == null) {
                group = new ArrayList<>();
                groups.put(w.getBackendGroup(), group);
            }

            group.add(w);
        }

        return groups;
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
    public void bufferWrite(String backendGroup, WriteMetric write) throws InterruptedException,
            BufferEnqueueException, MetricFormatException {
        final NodeRegistryEntry node = findNodeRegistryEntry(write);

        if (node == null)
            throw new BufferEnqueueException("Could not match write to any known node.");

        writeBulkProcessor.enqueue(new BufferedWriteMetric(node, backendGroup, write.getSeries(), write.getData()));
    }

    /**
     * Perform a write that could be routed to other cluster nodes.
     *
     * @param writes
     *            Writes to perform.
     * @return A callback that will be fired when the write is done or failed.
     * @throws BackendOperationException
     */
    private Callback<WriteBatchResult> routeWrites(final String backendGroup, List<BufferedWriteMetric> writes)
            throws BackendOperationException {
        final List<Callback<WriteBatchResult>> callbacks = new ArrayList<>();

        callbacks.addAll(writeCluster(backendGroup, writes));

        final Callback.Reducer<WriteBatchResult, WriteBatchResult> reducer = new Callback.Reducer<WriteBatchResult, WriteBatchResult>() {
            @Override
            public WriteBatchResult resolved(Collection<WriteBatchResult> results, Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                for (final Exception e : errors) {
                    log.error("Remote write failed", e);
                }

                for (final CancelReason reason : cancelled) {
                    log.error("Remote write cancelled: " + reason.getMessage());
                }

                boolean ok = errors.isEmpty() && cancelled.isEmpty();

                if (ok) {
                    for (final WriteBatchResult r : results) {
                        ok = ok && r.isOk();
                    }
                }

                return new WriteBatchResult(ok, results.size() + errors.size() + cancelled.size());
            }
        };

        return ConcurrentCallback.newReduce(callbacks, reducer);
    }

    private List<Callback<WriteBatchResult>> writeCluster(final String backendGroup,
            final List<BufferedWriteMetric> writes) throws BackendOperationException {
        final List<Callback<WriteBatchResult>> callbacks = new ArrayList<>();

        final Multimap<NodeRegistryEntry, WriteMetric> partitions = LinkedListMultimap.create();

        for (final BufferedWriteMetric w : writes) {
            partitions.put(w.getNode(), new WriteMetric(w.getSeries(), w.getData()));
        }

        for (final Map.Entry<NodeRegistryEntry, Collection<WriteMetric>> entry : partitions.asMap().entrySet()) {
            callbacks.add(entry.getKey().getClusterNode().write(backendGroup, entry.getValue()));
        }

        return callbacks;
    }

    public Callback<WriteBatchResult> write(MetricBackendGroup backend, Collection<WriteMetric> writes) {
        final List<Callback<WriteBatchResult>> callbacks = new ArrayList<>();

        callbacks.add(backend.write(writes));

        if (callbacks.isEmpty())
            return new CancelledCallback<WriteBatchResult>(CancelReason.NO_BACKENDS_AVAILABLE);

        return ConcurrentCallback.newReduce(callbacks, WriteBatchResult.merger());
    }

    private NodeRegistryEntry findNodeRegistryEntry(final WriteMetric write) throws MetricFormatException {
        if (!cluster.isReady())
            return null;

        final NodeRegistryEntry node = cluster.findNode(write.getSeries().getTags(), NodeCapability.WRITE);

        if (node == null)
            throw new MetricFormatException("Could not route: " + write.getSeries());

        return node;
    }

    public Callback<QueryMetricsResult> queryMetrics(final String backendGroup, final Filter filter,
            final List<String> groupBy, final DateRange range, final AggregationGroup aggregation)
            throws MetricQueryException {

        final Collection<NodeRegistryEntry> nodes = cluster.findAllShards(NodeCapability.QUERY);

        final List<Callback<MetricGroups>> callbacks = Lists.newArrayList();

        final DateRange rounded = roundRange(aggregation, range);

        for (final NodeRegistryEntry n : nodes) {
            final Filter f = modifyFilter(n.getMetadata(), filter);

            final Map<String, String> shard = n.getMetadata().getTags();

            final Callback<MetricGroups> query = n.getClusterNode().fullQuery(backendGroup, f, groupBy, rounded,
                    aggregation);

            callbacks.add(query.transform(MetricGroups.identity(),
                    MetricGroups.nodeError(n.getMetadata().getId(), n.getUri(), shard)));
        }

        return ConcurrentCallback.newReduce(callbacks, MetricGroups.merger())
                .transform(new MetricGroupsTransformer(rounded)).register(reporter.reportQueryMetrics());
    }

    private Filter modifyFilter(NodeMetadata metadata, Filter filter) {
        final List<Filter> statements = new ArrayList<>();
        statements.add(filter);

        for (final Map.Entry<String, String> entry : metadata.getTags().entrySet()) {
            statements.add(new MatchTagFilter(entry.getKey(), entry.getValue()));
        }

        return new AndFilter(statements).optimize();
    }

    public static interface StreamingQuery {
        public Callback<MetricGroups> query(final DateRange range);
    }

    /**
     * Check if the query wants to hint at a specific interval. If that is the case, round the provided date to the
     * specified interval.
     *
     * @param query
     * @return
     */
    private DateRange roundRange(AggregationGroup aggregation, DateRange range) {
        if (aggregation == null)
            return range;

        final Sampling sampling = aggregation.getSampling();
        return range.rounded(sampling.getExtent()).rounded(sampling.getSize()).shiftStart(-sampling.getExtent());
    }

    private Callback<FindTimeSeriesGroups> findAllTimeSeries(final Filter filter, List<String> groupBy) {
        final FindSeriesTransformer transformer = new FindSeriesTransformer(groupBy);
        return metadata.findSeries(filter).transform(transformer);
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
        }, flushingInterval, TimeUnit.MILLISECONDS);
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
