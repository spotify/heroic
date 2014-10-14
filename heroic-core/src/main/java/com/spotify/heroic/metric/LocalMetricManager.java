package com.spotify.heroic.metric;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.Multimap;
import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.aggregationcache.AggregationCache;
import com.spotify.heroic.async.DelayedTransform;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metric.exceptions.BackendOperationException;
import com.spotify.heroic.metric.model.BufferedWriteMetric;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.statistics.LocalMetricManagerReporter;
import com.spotify.heroic.statistics.MetricBackendsReporter;
import com.spotify.heroic.utils.BackendGroups;

@Slf4j
@NoArgsConstructor
@ToString(exclude = { "scheduledExecutor" })
public class LocalMetricManager implements MetricManager, LifeCycle {
    @Inject
    @Named("backends")
    private BackendGroups<MetricBackend> backends;

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
    private MetricBackendsReporter backendsReporter;

    @Inject
    private LocalMetricManagerReporter reporter;

    @Inject
    private AggregationCache cache;

    @Inject
    private MetadataManager metadata;

    @Inject
    private ScheduledExecutorService scheduledExecutor;

    private final MetricBulkProcessor<BufferedWriteMetric> writeBulkProcessor = new MetricBulkProcessor<>(
            new MetricBulkProcessor.Flushable<BufferedWriteMetric>() {
                @Override
                public void flushWrites(List<BufferedWriteMetric> writes) throws Exception {
                    log.info("Flushing {} write(s)", writes.size());
                    LocalMetricManager.this.flushWrites(writes);
                }
            });

    @Override
    public List<MetricBackend> getBackends() {
        return backends.all();
    }

    /**
     * Perform a write that could be routed to other cluster nodes.
     *
     * @param writes
     *            Writes to perform.
     * @return A callback that will be fired when the write is done or failed.
     * @throws BackendOperationException
     */
    private Future<WriteBatchResult> routeWrites(final String backendGroup, List<BufferedWriteMetric> writes)
            throws BackendOperationException {
        final List<Future<WriteBatchResult>> callbacks = new ArrayList<>();

        callbacks.addAll(writeCluster(backendGroup, writes));

        return Futures.reduce(callbacks, WriteBatchResult.merger());
    }

    private List<Future<WriteBatchResult>> writeCluster(final String backendGroup,
            final List<BufferedWriteMetric> writes) throws BackendOperationException {
        final List<Future<WriteBatchResult>> callbacks = new ArrayList<>();

        final Multimap<NodeRegistryEntry, WriteMetric> partitions = LinkedListMultimap.create();

        for (final BufferedWriteMetric w : writes) {
            partitions.put(w.getNode(), new WriteMetric(w.getSeries(), w.getData()));
        }

        for (final Map.Entry<NodeRegistryEntry, Collection<WriteMetric>> entry : partitions.asMap().entrySet()) {
            callbacks.add(entry.getKey().getClusterNode().write(backendGroup, entry.getValue()));
        }

        return callbacks;
    }

    @Override
    public Future<MetricGroups> queryMetrics(final String backendGroup, final Filter filter,
            final List<String> groupBy, final DateRange range, final AggregationGroup aggregation) {
        return metadata.findSeries(filter).register(reporter.reportFindSeries())
                .transform(runQueries(groupBy, backendGroup, filter, range, aggregation))
                .register(reporter.reportQueryMetrics());
    }

    private DelayedTransform<FindSeries, MetricGroups> runQueries(final List<String> groupBy,
            final String backendGroup, final Filter filter, final DateRange range, final AggregationGroup aggregation) {
        return new DelayedTransform<FindSeries, MetricGroups>() {
            @Override
            public Future<MetricGroups> transform(final FindSeries result) throws Exception {
                final Map<Map<String, String>, Set<Series>> groups = setupMetricGroups(groupBy, result);

                if (groups.size() > groupLimit)
                    throw new IllegalArgumentException("The current query is too heavy! (More than " + groupLimit
                            + " timeseries would be sent to your browser).");

                final List<Future<MetricGroups>> futures = new ArrayList<>();

                for (final Entry<Map<String, String>, Set<Series>> entry : groups.entrySet()) {
                    final Map<String, String> key = entry.getKey();

                    try {
                        futures.add(runQuery(key, backendGroup, filter, range, aggregation, entry.getValue()));
                    } catch (final Exception e) {
                        futures.add(Futures.resolved(MetricGroups.seriesError(key, e)));
                    }
                }

                return Futures.reduce(futures, MetricGroups.merger());
            }

            private Future<MetricGroups> runQuery(final Map<String, String> key, final String backendGroup,
                    final Filter filter, final DateRange range, final AggregationGroup aggregation,
                    final Set<Series> series) throws BackendOperationException {
                if (series.isEmpty())
                    throw new IllegalArgumentException("No series found in group");

                if (series.size() > groupLoadLimit)
                    throw new IllegalArgumentException("More than " + groupLoadLimit
                            + " original time series would be loaded from the backend.");

                return useGroup(backendGroup).query(key, filter, series, range, aggregation);
            }

            private Map<Map<String, String>, Set<Series>> setupMetricGroups(final List<String> groupBy,
                    final FindSeries result) {
                final Map<Map<String, String>, Set<Series>> groups = new HashMap<>();

                for (final Series series : result.getSeries()) {
                    final Map<String, String> key = new HashMap<>();

                    if (groupBy != null) {
                        for (final String group : groupBy) {
                            key.put(group, series.getTags().get(group));
                        }
                    }

                    Set<Series> group = groups.get(key);

                    if (group == null) {
                        group = new HashSet<>();
                        groups.put(key, group);
                    }

                    group.add(series);
                }

                return groups;
            }
        };
    }

    @Override
    public MetricBackends useDefaultGroup() throws BackendOperationException {
        return useGroup(null);
    }

    @Override
    public MetricBackends useGroup(final String group) throws BackendOperationException {
        final List<MetricBackend> selected;

        if (group == null) {
            final List<MetricBackend> defaults = backends.defaults();

            if (defaults == null)
                throw new BackendOperationException("No default backend configured");

            selected = defaults;
        } else {
            selected = backends.find(group);
        }

        if (selected == null || selected.isEmpty())
            throw new BackendOperationException("No usable metric backends available");

        return useAlive(selected);
    }

    @Override
    public MetricBackends useGroups(final Set<String> groups) throws BackendOperationException {
        final List<MetricBackend> selected;

        if (groups == null) {
            final List<MetricBackend> defaults = backends.defaults();

            if (defaults == null)
                throw new BackendOperationException("No default backend configured");

            selected = defaults;
        } else {
            selected = backends.find(groups);
        }

        if (selected == null || selected.isEmpty())
            throw new BackendOperationException("No usable metric backends available");

        return useAlive(selected);
    }

    private MetricBackends useAlive(List<MetricBackend> backends) throws BackendOperationException {
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

        return new MetricBackendsImpl(backendsReporter, cache, disabled, alive);
    }

    @Override
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

    private void flushWrites(List<BufferedWriteMetric> bufferedWrites) throws Exception {
        final Map<String, List<BufferedWriteMetric>> writes = groupByBackendGroup(bufferedWrites);

        final List<Future<WriteBatchResult>> callbacks = new ArrayList<>();

        for (final Map.Entry<String, List<BufferedWriteMetric>> entry : writes.entrySet()) {
            callbacks.add(routeWrites(entry.getKey(), entry.getValue()));
        }

        final Future<WriteBatchResult> callback = Futures.reduce(callbacks, WriteBatchResult.merger());

        final WriteBatchResult result;

        try {
            result = callback.register(reporter.reportFlushWrites()).get();
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
}
