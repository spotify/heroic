package com.spotify.heroic.metric;

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

import javax.inject.Inject;
import javax.inject.Named;

import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregationcache.AggregationCache;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metadata.ClusteredMetadataManager;
import com.spotify.heroic.metric.async.FindSeriesTransformer;
import com.spotify.heroic.metric.async.LocalFindAndRouteTransformer;
import com.spotify.heroic.metric.async.PreparedQueryTransformer;
import com.spotify.heroic.metric.error.BackendOperationException;
import com.spotify.heroic.metric.model.BufferedWriteMetric;
import com.spotify.heroic.metric.model.FindTimeSeriesGroups;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.PreparedQuery;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;

@Slf4j
@NoArgsConstructor
@ToString(exclude = { "scheduledExecutor" })
public class MetricBackendManager implements LifeCycle {
    @Inject
    @Named("backends")
    private Map<String, List<MetricBackend>> backends;

    @Inject
    @Named("defaultBackends")
    private List<MetricBackend> defaultBackends;

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
    private AggregationCache cache;

    @Inject
    private ClusteredMetadataManager metadata;

    @Inject
    private ScheduledExecutorService scheduledExecutor;

    private final MetricBulkProcessor<BufferedWriteMetric> writeBulkProcessor = new MetricBulkProcessor<>(
            new MetricBulkProcessor.Flushable<BufferedWriteMetric>() {
                @Override
                public void flushWrites(List<BufferedWriteMetric> writes) throws Exception {
                    log.info("Flushing {} write(s)", writes.size());
                    MetricBackendManager.this.flushWrites(writes);
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

    public List<MetricBackend> findBackends(Set<String> groups) {
        final List<MetricBackend> result = new ArrayList<>();

        for (final String group : groups) {
            final List<MetricBackend> partial = findBackends(group);

            if (partial == null)
                continue;

            result.addAll(partial);
        }

        if (result.isEmpty())
            return null;

        return ImmutableList.copyOf(result);
    }

    public List<MetricBackend> findBackends(String group) {
        if (group == null)
            return null;

        final List<MetricBackend> result = backends.get(group);

        if (result == null || result.isEmpty())
            return null;

        return ImmutableList.copyOf(result);
    }

    public MetricBackend findOneBackend(String group) {
        final List<MetricBackend> result = findBackends(group);

        if (result.isEmpty())
            return null;

        if (result.size() > 1)
            throw new IllegalStateException("Too many backends matching '" + group + "' are available: " + result);

        return result.get(0);
    }

    public List<MetricBackend> getBackends() {
        final List<MetricBackend> result = new ArrayList<>();

        for (final Map.Entry<String, List<MetricBackend>> entry : backends.entrySet())
            result.addAll(entry.getValue());

        return ImmutableList.copyOf(result);
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

    public Callback<MetricGroups> directQueryMetrics(final String backendGroup, final Filter filter,
            final List<String> groupBy, final DateRange range, final AggregationGroup aggregation) {
        final PreparedQueryTransformer transformer = new PreparedQueryTransformer(range, aggregation);

        return findAndRouteTimeSeries(backendGroup, filter, groupBy).transform(transformer).register(
                reporter.reportQueryMetrics());
    }

    public static interface StreamingQuery {
        public Callback<MetricGroups> query(final DateRange range);
    }

    public MetricBackendGroup useDefaultGroup() throws BackendOperationException {
        return useGroup(null);
    }

    public MetricBackendGroup useGroup(final String group) throws BackendOperationException {
        final List<MetricBackend> selected;

        if (group == null) {
            if (defaultBackends == null)
                throw new BackendOperationException("No default backend configured");

            selected = defaultBackends;
        } else {
            selected = findBackends(group);
        }

        if (selected == null || selected.isEmpty())
            throw new BackendOperationException("No usable metric backends available");

        return useAlive(selected);
    }

    public MetricBackendGroup useGroups(final Set<String> groups) throws BackendOperationException {
        final List<MetricBackend> selected;

        if (groups == null) {
            if (defaultBackends == null)
                throw new BackendOperationException("No default backend configured");

            selected = defaultBackends;
        } else {
            selected = findBackends(groups);
        }

        if (selected == null || selected.isEmpty())
            throw new BackendOperationException("No usable metric backends available");

        return useAlive(selected);
    }

    private MetricBackendGroup useAlive(List<MetricBackend> backends) throws BackendOperationException {
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

        if (alive.isEmpty())
            throw new BackendOperationException("No alive backends available");

        return new MetricBackendGroup(cache, reporter, disabled, alive);
    }

    /**
     * Finds time series and routing the query to a specific remote Heroic instance.
     *
     * @param criteria
     * @return
     */
    private Callback<List<PreparedQuery>> findAndRouteTimeSeries(final String backendGroup, final Filter filter,
            final List<String> groupBy) {
        return findAllTimeSeries(filter, groupBy).transform(
                new LocalFindAndRouteTransformer(this, filter, backendGroup, groupLimit, groupLoadLimit)).register(
                reporter.reportFindTimeSeries());
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
