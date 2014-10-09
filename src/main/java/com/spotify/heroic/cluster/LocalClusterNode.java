package com.spotify.heroic.cluster;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.NoArgsConstructor;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.error.BackendOperationException;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@NoArgsConstructor
public class LocalClusterNode implements ClusterNode {
    @Inject
    private MetricManager metrics;

    @Inject
    private MetadataManager localMetadata;

    @Inject
    @Named("localId")
    private UUID id;

    @Override
    public Callback<MetricGroups> query(final String backendGroup, final Filter filter,
            final Map<String, String> group, final AggregationGroup aggregation, final DateRange range,
            final Set<Series> series) {
        try {
            return metrics.useGroup(backendGroup).groupedQuery(group, filter, series, range, aggregation);
        } catch (final BackendOperationException e) {
            return new FailedCallback<>(e);
        }
    }

    @Override
    public Callback<WriteBatchResult> write(String backendGroup, Collection<WriteMetric> writes) {
        try {
            return metrics.useGroup(backendGroup).write(writes);
        } catch (final BackendOperationException e) {
            return new FailedCallback<>(e);
        }
    }

    @Override
    public Callback<MetricGroups> fullQuery(String backendGroup, Filter filter, List<String> groupBy, DateRange range,
            AggregationGroup aggregation) {
        return metrics.directQueryMetrics(backendGroup, filter, groupBy, range, aggregation);
    }

    @Override
    public Callback<FindTags> findTags(Filter filter) {
        return localMetadata.findTags(filter);
    }

    @Override
    public Callback<FindKeys> findKeys(Filter filter) {
        return localMetadata.findKeys(filter);
    }

    @Override
    public Callback<FindSeries> findSeries(Filter filter) {
        return localMetadata.findSeries(filter);
    }

    @Override
    public Callback<DeleteSeries> deleteSeries(Filter filter) {
        return localMetadata.deleteSeries(filter);
    }

    @Override
    public Callback<String> writeSeries(Series series) {
        return localMetadata.bufferWrite(series);
    }
}
