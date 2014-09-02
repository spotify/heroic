package com.spotify.heroic.metrics.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.metrics.model.FindTimeSeriesGroups;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.PreparedQuery;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@Slf4j
@RequiredArgsConstructor
public final class FindAndRouteTransformer implements
Callback.Transformer<FindTimeSeriesGroups, List<PreparedQuery>> {
    @RequiredArgsConstructor
    public static class ClusterQuery implements PreparedQuery {
        private final String backendGroup;
        private final ClusterNode node;
        private final Series key;
        private final Set<Series> series;

        @Override
        public Callback<MetricGroups> query(final DateRange range,
                final AggregationGroup aggregationGroup) {
            return node.query(backendGroup, key, series, range,
                    aggregationGroup);
        }
    };

    private final String backendGroup;
    private final int groupLimit;
    private final int groupLoadLimit;
    private final ClusterManager cluster;

    @Override
    public List<PreparedQuery> transform(final FindTimeSeriesGroups result)
            throws Exception {
        final List<PreparedQuery> queries = new ArrayList<>();

        final Map<Series, Set<Series>> groups = result.getGroups();

        if (groups.size() > groupLimit)
            throw new IllegalArgumentException(
                    "The current query is too heavy! (More than " + groupLimit
                    + " timeseries would be sent to your browser).");

        for (final Entry<Series, Set<Series>> group : groups.entrySet()) {
            final Set<Series> timeseries = group.getValue();

            if (timeseries.isEmpty())
                continue;

            if (timeseries.size() > groupLoadLimit)
                throw new IllegalArgumentException(
                        "The current query is too heavy! (More than "
                                + groupLoadLimit
                                + " original time series would be loaded from Cassandra).");

            final PreparedQuery query = clusterQuery(group.getKey(), timeseries);

            if (query == null)
                continue;

            queries.add(query);
        }

        return queries;
    }

    public PreparedQuery clusterQuery(Series key, Set<Series> timeseries) {
        final Series one = timeseries.iterator().next();

        final NodeRegistryEntry node = cluster.findNode(one.getTags(),
                NodeCapability.QUERY);

        if (node == null) {
            log.warn("No matching node in group {} found for {}", key,
                    one.getTags());
            return null;
        }

        for (final Series series : timeseries) {
            if (!node.getMetadata().matchesTags(series.getTags()))
                throw new IllegalArgumentException(
                        "The current query is too heavy! (Global aggregation not permitted)");
        }

        final ClusterNode clusterNode = node.getClusterNode();

        return new ClusterQuery(backendGroup, clusterNode, key, timeseries);
    }
}