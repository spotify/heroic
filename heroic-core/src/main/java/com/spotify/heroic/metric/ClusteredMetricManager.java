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
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.async.Transform;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.model.NodeCapability;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.filter.AndFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MatchTagFilter;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metadata.ClusteredMetadataManager;
import com.spotify.heroic.metric.exceptions.BufferEnqueueException;
import com.spotify.heroic.metric.exceptions.MetricFormatException;
import com.spotify.heroic.metric.exceptions.MetricQueryException;
import com.spotify.heroic.metric.model.BufferedWriteMetric;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.QueryMetricsResult;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.statistics.MetricManagerReporter;

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
    private MetricManagerReporter reporter;

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
                    ClusteredMetricManager.this.write(writes);
                }
            });

    private final Executor deferredExecutor = Executors.newFixedThreadPool(10);

    public void write(List<BufferedWriteMetric> bufferedWrites) throws Exception {
        final Map<String, List<BufferedWriteMetric>> writes = groupByBackendGroup(bufferedWrites);

        final List<Future<WriteBatchResult>> callbacks = new ArrayList<>();

        for (final Map.Entry<String, List<BufferedWriteMetric>> entry : writes.entrySet()) {
            final Multimap<NodeRegistryEntry, WriteMetric> partitions = LinkedListMultimap.create();

            for (final BufferedWriteMetric w : entry.getValue()) {
                partitions.put(w.getNode(), new WriteMetric(w.getSeries(), w.getData()));
            }

            for (final Map.Entry<NodeRegistryEntry, Collection<WriteMetric>> e : partitions.asMap().entrySet()) {
                callbacks.add(e.getKey().getClusterNode().write(entry.getKey(), e.getValue()));
            }
        }

        final WriteBatchResult result = Futures.reduce(callbacks, WriteBatchResult.merger()).get();

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
    public void write(String backendGroup, WriteMetric write) throws InterruptedException, BufferEnqueueException,
            MetricFormatException {
        final NodeRegistryEntry node = cluster.findNode(write.getSeries().getTags(), NodeCapability.WRITE);

        if (node == null)
            throw new BufferEnqueueException("Could not find node to write to for: " + write.getSeries());

        writeBulkProcessor.enqueue(new BufferedWriteMetric(node, backendGroup, write.getSeries(), write.getData()));
    }

    public Future<QueryMetricsResult> query(final String backendGroup, final Filter filter, final List<String> groupBy,
            final DateRange range, final AggregationGroup aggregation) throws MetricQueryException {

        final Collection<NodeRegistryEntry> nodes = cluster.findAllShards(NodeCapability.QUERY);

        final List<Future<MetricGroups>> callbacks = Lists.newArrayList();

        final DateRange rounded = roundRange(aggregation, range);

        for (final NodeRegistryEntry n : nodes) {
            final Filter f = modifyFilter(n.getMetadata(), filter);

            final Map<String, String> shard = n.getMetadata().getTags();

            final Future<MetricGroups> query = n.getClusterNode().fullQuery(backendGroup, f, groupBy, rounded,
                    aggregation);

            callbacks.add(query.transform(MetricGroups.identity(),
                    MetricGroups.nodeError(n.getMetadata().getId(), n.getUri(), shard)));
        }

        return Futures.reduce(callbacks, MetricGroups.merger()).transform(toQueryMetricsResult(rounded))
                .register(reporter.reportQueryMetrics());
    }

    private Transform<MetricGroups, QueryMetricsResult> toQueryMetricsResult(final DateRange rounded) {
        return new Transform<MetricGroups, QueryMetricsResult>() {
            @Override
            public QueryMetricsResult transform(MetricGroups result) throws Exception {
                return new QueryMetricsResult(rounded, result);
            }
        };
    }

    private Filter modifyFilter(NodeMetadata metadata, Filter filter) {
        final List<Filter> statements = new ArrayList<>();
        statements.add(filter);

        for (final Map.Entry<String, String> entry : metadata.getTags().entrySet()) {
            statements.add(new MatchTagFilter(entry.getKey(), entry.getValue()));
        }

        return new AndFilter(statements).optimize();
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
