package com.spotify.heroic.http.rpc4;

import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import lombok.Data;
import lombok.ToString;

import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.http.rpc.RpcPostRequestResolver;
import com.spotify.heroic.http.rpc.RpcWriteResult;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.metrics.model.WriteBatchResult;
import com.spotify.heroic.metrics.model.WriteMetric;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@Data
@ToString(exclude = { "config", "executor" })
public class Rpc4ClusterNode implements ClusterNode {
    private final String base;
    private final URI uri;

    private final ClientConfig config;
    private final Executor executor;

    private <R, T> Callback<T> resolve(R request, Class<T> clazz,
            String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(uri).path(base).path(endpoint);
        return ConcurrentCallback.newResolve(executor,
                new RpcPostRequestResolver<R, T>(request, clazz, target));
    }

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
        return resolve(request, Rpc4MetricGroups.class, "query").transform(
                QUERY);
    }

    @Override
    public Callback<WriteBatchResult> write(final String backendGroup,
            Collection<WriteMetric> writes) {
        final Rpc4WriteBody request = new Rpc4WriteBody(backendGroup, writes);
        return resolve(request, RpcWriteResult.class, "write").transform(WRITE);
    }

    @Override
    public Callback<MetricGroups> fullQuery(String backendGroup, Filter filter,
            List<String> groupBy, DateRange range, AggregationGroup aggregation) {
        final Rpc4FullQueryBody request = new Rpc4FullQueryBody(backendGroup,
                filter, groupBy, range, aggregation);
        return resolve(request, Rpc4MetricGroups.class, "full-query")
                .transform(QUERY);
    }
}
