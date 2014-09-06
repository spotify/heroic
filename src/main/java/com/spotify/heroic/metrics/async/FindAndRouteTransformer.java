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
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metrics.MetricBackendManager;
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
        private final Filter filter;
        private final Map<String, String> group;
        private final Set<Series> series;

        @Override
        public Callback<MetricGroups> query(final DateRange range,
                final AggregationGroup aggregation) {
            return node.query(backendGroup, filter, group, aggregation, range,
                    series);
        }
    };

    @RequiredArgsConstructor
    public static class LocalQuery implements PreparedQuery {
        private final MetricBackendManager metrics;
        private final String backendGroup;
        private final Filter filter;
        private final Map<String, String> group;
        private final Set<Series> series;

        @Override
        public Callback<MetricGroups> query(final DateRange range,
                final AggregationGroup aggregation) throws Exception {
            return metrics.useGroup(backendGroup).groupedQuery(group, filter,
                    series, range, aggregation);
        }
    };

    private final MetricBackendManager metrics;
    private final ClusterManager cluster;
    private final boolean localQuery;
    private final Filter filter;
    private final String backendGroup;
    private final int groupLimit;
    private final int groupLoadLimit;

    @Override
    public List<PreparedQuery> transform(final FindTimeSeriesGroups result)
            throws Exception {
        final List<PreparedQuery> queries = new ArrayList<>();

        final Map<Map<String, String>, Set<Series>> groups = result.getGroups();

        if (groups.size() > groupLimit)
            throw new IllegalArgumentException(
                    "The current query is too heavy! (More than " + groupLimit
                    + " timeseries would be sent to your browser).");

        for (final Entry<Map<String, String>, Set<Series>> entry : groups
                .entrySet()) {
            final Set<Series> series = entry.getValue();

            if (series.isEmpty())
                continue;

            if (series.size() > groupLoadLimit)
                throw new IllegalArgumentException(
                        "The current query is too heavy! (More than "
                                + groupLoadLimit
                                + " original time series would be loaded from Cassandra).");

            final PreparedQuery query;

            if (localQuery) {
                query = localQuery(filter, entry.getKey(), series);
            } else {
                query = clusterQuery(filter, entry.getKey(), series);
            }

            if (query == null)
                continue;

            queries.add(query);
        }

        return queries;
    }

    public PreparedQuery localQuery(Filter filter, Map<String, String> group,
            Set<Series> series) {
        return new LocalQuery(metrics, backendGroup, filter, group, series);
    }

    public PreparedQuery clusterQuery(Filter filter, Map<String, String> group,
            Set<Series> series) {
        final Series one = series.iterator().next();

        final NodeRegistryEntry node = cluster.findNode(one.getTags(),
                NodeCapability.QUERY);

        if (node == null) {
            log.warn("No matching node in group {} found for {}", group,
                    one.getTags());
            return null;
        }

        for (final Series s : series) {
            if (!node.getMetadata().matchesTags(s.getTags()))
                throw new IllegalArgumentException(
                        "The current query is too heavy! (Global aggregation not permitted)");
        }

        final ClusterNode clusterNode = node.getClusterNode();

        return new ClusterQuery(backendGroup, clusterNode, filter, group,
                series);
    }
}