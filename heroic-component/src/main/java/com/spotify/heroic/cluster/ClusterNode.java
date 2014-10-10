package com.spotify.heroic.cluster;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Future;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

public interface ClusterNode {
    public Future<MetricGroups> query(final String backendGroup, final Filter filter,
            final Map<String, String> group, final AggregationGroup aggregation, final DateRange range,
            final Set<Series> series);

    public Future<WriteBatchResult> write(final String backendGroup, Collection<WriteMetric> writes);

    public Future<MetricGroups> fullQuery(String backendGroup, Filter filter, List<String> groupBy, DateRange range,
            AggregationGroup aggregation);

    public Future<FindTags> findTags(Filter filter);

    public Future<FindKeys> findKeys(Filter filter);

    public Future<FindSeries> findSeries(Filter filter);

    public Future<DeleteSeries> deleteSeries(Filter filter);

    public Future<String> writeSeries(Series series);
}
