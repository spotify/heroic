package com.spotify.heroic.http.rpc1;

import java.net.URI;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.aggregation.AggregationGroup;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.metrics.model.MetricGroups;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.WriteMetric;
import com.spotify.heroic.model.WriteResult;

@Data
@ToString(of = "url")
public class Rpc1ClusterNode implements ClusterNode {
    private final String BASE = "rpc1";

    private final URI url;
    private final ClientConfig config;
    private final Executor executor;

    @RequiredArgsConstructor
    private final class QueryResolver implements
    Callback.Resolver<MetricGroups> {
        private final Rpc1QueryBody request;
        private final Client client;

        @Override
        public MetricGroups resolve() throws Exception {
            final WebTarget target = client.target(url).path(BASE)
                    .path("query");
            return target.request().post(
                    Entity.entity(request, MediaType.APPLICATION_JSON),
                    MetricGroups.class);
        }
    }

    @Override
    public Callback<MetricGroups> query(final Series key,
            final Set<Series> series, final DateRange range,
            final AggregationGroup aggregationGroup) {
        final Rpc1QueryBody request = new Rpc1QueryBody(key, series, range,
                aggregationGroup);
        return ConcurrentCallback.newResolve(executor, new QueryResolver(
                request, ClientBuilder.newClient(config)));
    }

    @RequiredArgsConstructor
    private final class WriteResolver implements Callback.Resolver<WriteResult> {
        private final List<WriteMetric> request;
        private final Client client;

        @Override
        public WriteResult resolve() throws Exception {
            final WebTarget target = client.target(url).path(BASE)
                    .path("write");
            return target.request().post(
                    Entity.entity(request, MediaType.APPLICATION_JSON),
                    WriteResult.class);
        }
    }

    @Override
    public Callback<WriteResult> write(List<WriteMetric> request) {
        return ConcurrentCallback.newResolve(executor, new WriteResolver(
                request, ClientBuilder.newClient(config)));
    }
}
