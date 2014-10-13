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
import com.spotify.heroic.async.Future;
import com.spotify.heroic.async.Futures;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.exceptions.BackendOperationException;
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
    public Future<MetricGroups> query(final String backendGroup, final Filter filter, final Map<String, String> group,
            final AggregationGroup aggregation, final DateRange range, final Set<Series> series) {
        try {
            return metrics.useGroup(backendGroup).query(group, filter, series, range, aggregation);
        } catch (final BackendOperationException e) {
            return Futures.failed(e);
        }
    }

    @Override
    public Future<WriteBatchResult> write(String backendGroup, Collection<WriteMetric> writes) {
        try {
            return metrics.useGroup(backendGroup).write(writes);
        } catch (final BackendOperationException e) {
            return Futures.failed(e);
        }
    }

    @Override
    public Future<MetricGroups> fullQuery(String backendGroup, Filter filter, List<String> groupBy, DateRange range,
            AggregationGroup aggregation) {
        return metrics.queryMetrics(backendGroup, filter, groupBy, range, aggregation);
    }

    @Override
    public Future<FindTags> findTags(Filter filter) {
        return localMetadata.findTags(filter);
    }

    @Override
    public Future<FindKeys> findKeys(Filter filter) {
        return localMetadata.findKeys(filter);
    }

    @Override
    public Future<FindSeries> findSeries(Filter filter) {
        return localMetadata.findSeries(filter);
    }

    @Override
    public Future<DeleteSeries> deleteSeries(Filter filter) {
        return localMetadata.deleteSeries(filter);
    }

    @Override
    public Future<String> writeSeries(Series series) {
        return localMetadata.bufferWrite(series);
    }
}
