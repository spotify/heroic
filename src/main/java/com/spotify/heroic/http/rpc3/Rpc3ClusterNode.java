package com.spotify.heroic.http.rpc3;

import java.net.URI;
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
@ToString(of = "url")
public class Rpc3ClusterNode implements ClusterNode {
    private final String BASE = "rpc3";

    private final URI url;
    private final ClientConfig config;
    private final Executor executor;

    private <R, T> Callback<T> resolve(R request, Class<T> clazz,
            String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(url).path(BASE).path(endpoint);
        return ConcurrentCallback.newResolve(executor,
                new RpcPostRequestResolver<R, T>(request, clazz, target));
    }

    @Override
    public Callback<MetricGroups> query(final String backendGroup,
            final Filter filter, final Map<String, String> group,
            final AggregationGroup aggregation, final DateRange range,
            final Set<Series> series) {
        final Rpc3QueryBody request = new Rpc3QueryBody(backendGroup, group,
                filter, series, range, aggregation);
        return resolve(request, MetricGroups.class, "query");
    }

    private static final Callback.Transformer<RpcWriteResult, WriteBatchResult> WRITE_TRANSFORMER = new Callback.Transformer<RpcWriteResult, WriteBatchResult>() {
        @Override
        public WriteBatchResult transform(RpcWriteResult result)
                throws Exception {
            return new WriteBatchResult(result.isOk(), 1);
        }
    };

    @Override
    public Callback<WriteBatchResult> write(final String backendGroup,
            List<WriteMetric> writes) {
        final Rpc3WriteBody request = new Rpc3WriteBody(backendGroup, writes);
        return resolve(request, RpcWriteResult.class, "write").transform(
                WRITE_TRANSFORMER);
    }
}
