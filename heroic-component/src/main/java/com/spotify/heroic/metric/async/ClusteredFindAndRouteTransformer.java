package com.spotify.heroic.metric.async;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Transformer;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeCapability;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.model.FindTimeSeriesGroups;
import com.spotify.heroic.metric.model.PreparedQuery;
import com.spotify.heroic.model.Series;

@Slf4j
@RequiredArgsConstructor
public final class ClusteredFindAndRouteTransformer implements
        Transformer<FindTimeSeriesGroups, List<PreparedQuery>> {
    private final ClusterManager cluster;
    private final Filter filter;
    private final String backendGroup;
    private final int groupLimit;
    private final int groupLoadLimit;

    @Override
    public List<PreparedQuery> transform(final FindTimeSeriesGroups result) throws Exception {
        final List<PreparedQuery> queries = new ArrayList<>();

        final Map<Map<String, String>, Set<Series>> groups = result.getGroups();

        if (groups.size() > groupLimit)
            throw new IllegalArgumentException("The current query is too heavy! (More than " + groupLimit
                    + " timeseries would be sent to your browser).");

        for (final Entry<Map<String, String>, Set<Series>> entry : groups.entrySet()) {
            final Set<Series> series = entry.getValue();

            if (series.isEmpty())
                continue;

            if (series.size() > groupLoadLimit)
                throw new IllegalArgumentException("The current query is too heavy! (More than " + groupLoadLimit
                        + " original time series would be loaded from Cassandra).");

            final PreparedQuery query = clusterQuery(filter, entry.getKey(), series);

            if (query == null)
                continue;

            queries.add(query);
        }

        return queries;
    }

    public PreparedQuery clusterQuery(Filter filter, Map<String, String> group, Set<Series> series) {
        final Series one = series.iterator().next();

        final NodeRegistryEntry node = cluster.findNode(one.getTags(), NodeCapability.QUERY);

        if (node == null) {
            log.warn("No matching node in group {} found for {}", group, one.getTags());
            return null;
        }

        for (final Series s : series) {
            if (!node.getMetadata().matchesTags(s.getTags()))
                throw new IllegalArgumentException("The current query is too heavy! (Global aggregation not permitted)");
        }

        final ClusterNode clusterNode = node.getClusterNode();

        return new ClusterQuery(backendGroup, clusterNode, filter, group, series);
    }
}