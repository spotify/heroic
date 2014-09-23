package com.spotify.heroic.http.rpc5;

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
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@Data
public class Rpc5ClusterNode implements ClusterNode {
    private final HttpClientSession client;

    private static final Callback.Transformer<Rpc5MetricGroups, MetricGroups> QUERY = new Callback.Transformer<Rpc5MetricGroups, MetricGroups>() {
        @Override
        public MetricGroups transform(Rpc5MetricGroups result) throws Exception {
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
        final Rpc5QueryBody request = new Rpc5QueryBody(backendGroup, group,
                filter, series, range, aggregation);
        return client.post(request, Rpc5MetricGroups.class, "query").transform(
                QUERY);
    }

    @Override
    public Callback<WriteBatchResult> write(final String backendGroup,
            Collection<WriteMetric> writes) {
        final Rpc5WriteBody request = new Rpc5WriteBody(backendGroup, writes);
        return client.post(request, RpcWriteResult.class, "write").transform(
                WRITE);
    }

    @Override
    public Callback<MetricGroups> fullQuery(String backendGroup, Filter filter,
            List<String> groupBy, DateRange range, AggregationGroup aggregation) {
        final Rpc5FullQueryBody request = new Rpc5FullQueryBody(backendGroup,
                filter, groupBy, range, aggregation);
        return client.post(request, Rpc5MetricGroups.class, "full-query")
                .transform(QUERY);
    }

    @Override
    public Callback<FindTags> findTags(Filter filter) {
        return client.post(filter, FindTags.class, "find-tags");
    }

    @Override
    public Callback<FindKeys> findKeys(Filter filter) {
        return client.post(filter, FindKeys.class, "find-keys");
    }

    @Override
    public Callback<FindSeries> findSeries(Filter filter) {
        return client.post(filter, FindSeries.class, "find-series");
    }

    @Override
    public Callback<DeleteSeries> deleteSeries(Filter filter) {
        return client.post(filter, DeleteSeries.class, "delete-series");
    }

    @Override
    public Callback<String> writeSeries(Series series) {
        return client.post(series, String.class, "write-series");
    }
}
