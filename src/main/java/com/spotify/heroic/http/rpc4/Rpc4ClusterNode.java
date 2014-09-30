package com.spotify.heroic.http.rpc4;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.http.HttpClientSession;
import com.spotify.heroic.http.rpc.RpcWriteResult;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.model.MetricGroups;
import com.spotify.heroic.metric.model.WriteBatchResult;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@Data
public class Rpc4ClusterNode implements ClusterNode {
    private final HttpClientSession client;

    private final MetadataBackendManager localMetadata;

    private static final Callback.Transformer<Rpc4MetricGroups, MetricGroups> QUERY = new Callback.Transformer<Rpc4MetricGroups, MetricGroups>() {
        @Override
        public MetricGroups transform(Rpc4MetricGroups result) throws Exception {
            return MetricGroups.build(result.getGroups(),
                    result.getStatistics(), result.getErrors());
        }
    };

    private static final Callback.Transformer<RpcWriteResult, WriteBatchResult> WRITE = new Callback.Transformer<RpcWriteResult, WriteBatchResult>() {
        @Override
        public WriteBatchResult transform(RpcWriteResult result)
                throws Exception {
            return new WriteBatchResult(result.isOk(), 1);
        }
    };

    @Override
    public Callback<MetricGroups> query(final String backendGroup,
            final Filter filter, final Map<String, String> group,
            final AggregationGroup aggregation, final DateRange range,
            final Set<Series> series) {
        final Rpc4QueryBody request = new Rpc4QueryBody(backendGroup, group,
                filter, series, range, aggregation);
        return client.post(request, Rpc4MetricGroups.class, "query").transform(
                QUERY);
    }

    @Override
    public Callback<WriteBatchResult> write(final String backendGroup,
            Collection<WriteMetric> writes) {
        final Rpc4WriteBody request = new Rpc4WriteBody(backendGroup, writes);
        return client.post(request, RpcWriteResult.class, "write").transform(
                WRITE);
    }

    @Override
    public Callback<MetricGroups> fullQuery(String backendGroup, Filter filter,
            List<String> groupBy, DateRange range, AggregationGroup aggregation) {
        final Rpc4FullQueryBody request = new Rpc4FullQueryBody(backendGroup,
                filter, groupBy, range, aggregation);
        return client.post(request, Rpc4MetricGroups.class, "full-query")
                .transform(QUERY);
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
        return localMetadata.writeSeries(series);
    }
}
