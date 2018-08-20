package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.metrics.error.BackendOperationException;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
public class LocalClusterNode implements ClusterNode {
    @Inject
    private MetricBackendManager metrics;

    @Getter
    private final UUID id;

    @Override
    public Callback<MetricGroups> query(final String backendGroup,
            final Filter filter, final Map<String, String> group,
            final AggregationGroup aggregation, final DateRange range,
            final Set<Series> series) {
        try {
            return metrics.useGroup(backendGroup).groupedQuery(group, filter,
                    series, range, aggregation);
        } catch (final BackendOperationException e) {
            return new FailedCallback<>(e);
        }
    }

    @Override
    public Callback<WriteBatchResult> write(String backendGroup,
            Collection<WriteMetric> writes) {
        try {
            return metrics.write(metrics.useGroup(backendGroup), writes);
        } catch (final BackendOperationException e) {
            return new FailedCallback<>(e);
        }
    }

    @Override
    public Callback<MetricGroups> fullQuery(String backendGroup, Filter filter,
            List<String> groupBy, DateRange range, AggregationGroup aggregation) {
        return metrics.directQueryMetrics(backendGroup, filter, groupBy, range,
                aggregation);
    }

    @Override
    public URI getUri() {
        return null;
    }
}