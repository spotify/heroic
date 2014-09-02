package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.metrics.model.MetricGroup;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.PreparedQuery;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DateRange;

@Slf4j
@RequiredArgsConstructor
public final class PreparedQueryTransformer implements
Callback.DeferredTransformer<List<PreparedQuery>, MetricGroups> {
    private final ClusterManager cluster;
    private final DateRange rounded;
    private final AggregationGroup aggregation;

    @Override
    public Callback<MetricGroups> transform(List<PreparedQuery> queries)
            throws Exception {
        final List<Callback<MetricGroups>> callbacks = new ArrayList<>();

        for (final PreparedQuery query : queries) {
            callbacks.add(query.query(rounded, aggregation));
        }

        final Callback.Reducer<MetricGroups, MetricGroups> reducer = new Callback.Reducer<MetricGroups, MetricGroups>() {
            @Override
            public MetricGroups resolved(Collection<MetricGroups> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                for (final Exception e : errors) {
                    log.error("Remote request failed", e);
                }

                final Statistics.Rpc rpc = buildRpcStatistics(results, errors);

                final List<MetricGroup> groups = new ArrayList<>();
                Statistics statistics = Statistics.builder().rpc(rpc).build();

                for (final MetricGroups metricGroups : results) {
                    groups.addAll(metricGroups.getGroups());
                    statistics = statistics.merge(metricGroups.getStatistics());
                }

                return new MetricGroups(groups, statistics);
            }

            private Statistics.Rpc buildRpcStatistics(
                    Collection<MetricGroups> results,
                    Collection<Exception> errors) {
                final ClusterManager.Statistics statistics = cluster
                        .getStatistics();

                if (statistics == null)
                    return new Statistics.Rpc(results.size(), errors.size(), 0,
                            0, false);

                return new Statistics.Rpc(results.size(), errors.size(),
                        statistics.getOnlineNodes(),
                        statistics.getOfflineNodes(), true);
            }
        };

        return ConcurrentCallback.newReduce(callbacks, reducer);
    }
}