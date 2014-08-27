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
import com.spotify.heroic.metrics.model.RemoteGroupedTimeSeries;
import com.spotify.heroic.metrics.model.Statistics;
import com.spotify.heroic.model.DateRange;

@Slf4j
@RequiredArgsConstructor
public final class RemoteTimeSeriesTransformer
implements
        Callback.DeferredTransformer<List<RemoteGroupedTimeSeries>, MetricGroups> {
    private final ClusterManager cluster;
    private final DateRange rounded;
    private final AggregationGroup aggregation;

    @Override
    public Callback<MetricGroups> transform(
            List<RemoteGroupedTimeSeries> grouped) throws Exception {
        final List<Callback<MetricGroups>> callbacks = new ArrayList<>();

        for (final RemoteGroupedTimeSeries group : grouped) {
            callbacks.add(group.getNode().query(group.getKey(),
                    group.getSeries(), rounded, aggregation));
        }

        final Callback.Reducer<MetricGroups, MetricGroups> reducer = new Callback.Reducer<MetricGroups, MetricGroups>() {
            @Override
            public MetricGroups resolved(Collection<MetricGroups> results,
                    Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                for (final Exception e : errors) {
                    log.error("Remote request failed", e);
                }

                final ClusterManager.Statistics clusterStatistics = cluster
                        .getStatistics();
                final Statistics.Rpc rpc;

                if (clusterStatistics != null) {
                    rpc = new Statistics.Rpc(results.size(), errors.size(),
                            clusterStatistics.getOnlineNodes(),
                            clusterStatistics.getOfflineNodes(), true);
                } else {
                    rpc = new Statistics.Rpc(results.size(), errors.size(), 0,
                            0, false);
                }

                final List<MetricGroup> groups = new ArrayList<>();
                Statistics statistics = Statistics.builder().rpc(rpc).build();

                for (final MetricGroups metricGroups : results) {
                    groups.addAll(metricGroups.getGroups());
                    statistics = statistics.merge(metricGroups.getStatistics());
                }

                return new MetricGroups(groups, statistics);
            }
        };

        return ConcurrentCallback.newReduce(callbacks, reducer);
    }
}