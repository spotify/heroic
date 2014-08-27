package com.spotify.heroic.metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.async.FindTimeSeriesTransformer;
import com.spotify.heroic.metrics.async.MergeWriteResult;
import com.spotify.heroic.metrics.async.MetricGroupsTransformer;
import com.spotify.heroic.metrics.async.RemoteTimeSeriesTransformer;
import com.spotify.heroic.metrics.async.TimeSeriesTransformer;
import com.spotify.heroic.metrics.model.FindTimeSeriesCriteria;
import com.spotify.heroic.metrics.model.FindTimeSeriesGroups;
import com.spotify.heroic.metrics.model.GroupedTimeSeries;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.QueryMetricsResult;
import com.spotify.heroic.metrics.model.RemoteGroupedTimeSeries;
import com.spotify.heroic.metrics.model.StreamMetricsResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteMetric;
import com.spotify.heroic.model.WriteResult;
import com.spotify.heroic.model.filter.Filter;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;

@RequiredArgsConstructor
@Slf4j
public class MetricBackendManager {
    private final MetricBackendManagerReporter reporter;
    private final List<Backend> backends;
    private final boolean updateMetadata;
    private final int groupLimit;
    private final int groupLoadLimit;

    @Inject
    @Nullable
    private AggregationCache aggregationCache;

    @Inject
    private ClusterManager cluster;

    @Inject
    private MetadataBackendManager metadata;

    /**
     * Used for deferring work to avoid deep stack traces.
     */
    private final Executor deferredExecutor = Executors.newFixedThreadPool(10);

    private final class FindAndRouteTransformer
    implements
    Callback.Transformer<FindTimeSeriesGroups, List<RemoteGroupedTimeSeries>> {
        @Override
        public List<RemoteGroupedTimeSeries> transform(
                final FindTimeSeriesGroups result) throws Exception {
            final List<RemoteGroupedTimeSeries> grouped = new ArrayList<>();

            final Map<Series, Set<Series>> groups = result.getGroups();

            if (groups.size() > groupLimit)
                throw new IllegalArgumentException(
                        "The current query is too heavy! (More than "
                                + groupLimit
                                + " timeseries would be sent to your browser).");

            for (final Entry<Series, Set<Series>> group : groups.entrySet()) {
                final Set<Series> timeseries = group.getValue();

                if (timeseries.isEmpty())
                    continue;

                final Series one = timeseries.iterator().next();

                final NodeRegistryEntry node = cluster.findNode(one.getTags(),
                        NodeCapability.QUERY);

                if (node == null) {
                    log.warn("No matching node in group {} found for {}",
                            group.getKey(), one.getTags());
                    continue;
                }

                if (timeseries.size() > groupLoadLimit)
                    throw new IllegalArgumentException(
                            "The current query is too heavy! (More than "
                                    + groupLoadLimit
                                    + " original time series would be loaded from Cassandra).");

                for (final Series series : timeseries) {
                    if (!node.getMetadata().matchesTags(series.getTags()))
                        throw new IllegalArgumentException(
                                "The current query is too heavy! (Global aggregation not permitted)");
                }

                grouped.add(new RemoteGroupedTimeSeries(group.getKey(), group
                        .getValue(), node.getClusterNode()));
            }

            return grouped;
        }
    }

    interface BackendOperation {
        void run(int disabled, Backend backend) throws Exception;
    }

    public Callback<Boolean> write(final List<WriteMetric> writes)
            throws MetricWriteException {
        if (cluster == ClusterManager.NULL)
            throw new MetricWriteException(
                    "Cannot write metrics, cluster is not configured");

        final Map<NodeRegistryEntry, List<WriteMetric>> partitions = new HashMap<>();

        for (final WriteMetric write : writes) {
            final NodeRegistryEntry node = cluster.findNode(write.getSeries()
                    .getTags(), NodeCapability.WRITE);

            if (node == null)
                throw new MetricWriteException("Cannot route "
                        + write.getSeries() + " to any known, writable node");

            List<WriteMetric> partition = partitions.get(node);

            if (partition == null) {
                partition = new ArrayList<WriteMetric>();
                partitions.put(node, partition);
            }

            partition.add(write);
        }

        final List<Callback<Boolean>> callbacks = new ArrayList<>();

        for (final Map.Entry<NodeRegistryEntry, List<WriteMetric>> entry : partitions
                .entrySet()) {
            final NodeRegistryEntry node = entry.getKey();
            final List<WriteMetric> nodeWrites = entry.getValue();

            callbacks.add(node.getClusterNode().write(nodeWrites));
        }

        final Callback.Reducer<Boolean, Boolean> reducer = new Callback.Reducer<Boolean, Boolean>() {
            @Override
            public Boolean resolved(Collection<Boolean> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                for (final Exception e : errors) {
                    log.error("Remote write failed", e);
                }

                for (final CancelReason reason : cancelled) {
                    log.error("Remote write cancelled: " + reason.getMessage());
                }

                boolean ok = true;

                for (final Boolean b : results) {
                    ok &= b;
                }

                return ok;
            }
        };

        return ConcurrentCallback.newReduce(callbacks, reducer);
    }

    public Callback<QueryMetricsResult> queryMetrics(final Filter filter,
            final List<String> groupBy, final DateRange range,
            final AggregationGroup aggregation) throws MetricQueryException {
        if (cluster == ClusterManager.NULL)
            throw new MetricQueryException(
                    "Cannot query metrics, cluster is not configured");

        final DateRange rounded = roundRange(aggregation, range);

        final FindTimeSeriesCriteria criteria = new FindTimeSeriesCriteria(
                filter, groupBy, rounded);

        final RemoteTimeSeriesTransformer transformer = new RemoteTimeSeriesTransformer(
                cluster, rounded, aggregation);

        return findAndRouteTimeSeries(criteria).transform(transformer)
                .transform(new MetricGroupsTransformer(rounded))
                .register(reporter.reportQueryMetrics());
    }

    public Callback<StreamMetricsResult> streamMetrics(final Filter filter,
            final List<String> groupBy, final DateRange range,
            final AggregationGroup aggregation, MetricStream handle)
                    throws MetricQueryException {
        if (cluster == ClusterManager.NULL)
            throw new MetricQueryException(
                    "Cannot stream metrics, cluster is not configured");

        final DateRange rounded = roundRange(aggregation, range);

        final FindTimeSeriesCriteria criteria = new FindTimeSeriesCriteria(
                filter, groupBy, rounded);

        final Callback<List<RemoteGroupedTimeSeries>> rows = findAndRouteTimeSeries(criteria);

        final Callback<StreamMetricsResult> callback = new ConcurrentCallback<StreamMetricsResult>();

        final String streamId = Integer.toHexString(criteria.hashCode());

        log.info("{}: streaming {}", streamId, criteria);

        final StreamingQuery streamingQuery = new StreamingQuery() {
            @Override
            public Callback<MetricGroups> query(DateRange range) {
                log.info("{}: streaming chunk {}", streamId, range);

                final RemoteTimeSeriesTransformer transformer = new RemoteTimeSeriesTransformer(
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
    private Callback<List<RemoteGroupedTimeSeries>> findAndRouteTimeSeries(
            final FindTimeSeriesCriteria criteria) {
        return findAllTimeSeries(criteria).transform(
                new FindAndRouteTransformer()).register(
                        reporter.reportFindTimeSeries());
    }

    private Callback<FindTimeSeriesGroups> findAllTimeSeries(
            final FindTimeSeriesCriteria query) {
        final FindTimeSeriesTransformer transformer = new FindTimeSeriesTransformer(
                query.getGroupBy());
        return metadata.findTimeSeries(query.getFilter())
                .transform(transformer);
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
     * RPC METHODS.
     */

    public Callback<MetricGroups> directQuery(final Series key,
            final Set<Series> series, final DateRange range,
            final AggregationGroup aggregationGroup) {
        final TimeSeriesTransformer transformer = new TimeSeriesTransformer(
                aggregationCache, aggregationGroup, range);

        return groupTimeseries(key, series).transform(transformer).register(
                reporter.reportRpcQueryMetrics());
    }

    public Callback<WriteResult> directWrite(final List<WriteMetric> writes) {
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
}
