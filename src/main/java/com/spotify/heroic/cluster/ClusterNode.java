package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.concurrent.Executor;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import lombok.Data;
import lombok.ToString;

import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.http.rpc.model.RpcMetadata;
import com.spotify.heroic.http.rpc.model.RpcQueryBody;
import com.spotify.heroic.metrics.model.MetricGroups;

@Data
@ToString(of = "url")
public class ClusterNode {
    private final URI url;
    private final ClientConfig config;
    private final Executor executor;

    private final class GetMetadataResolver implements
            Callback.Resolver<NodeMetadata> {
        private final Client client;

        private GetMetadataResolver(Client client) {
            this.client = client;
        }

        @Override
        public NodeMetadata resolve() throws Exception {
            final WebTarget target = client.target(url).path("rpc")
                    .path("metadata");
            final RpcMetadata response = target.request().get(
                    RpcMetadata.class);
            return new NodeMetadata(response.getId(), response.getTags());
        }
    }

    public Callback<NodeMetadata> getMetadata() {
        return ConcurrentCallback.newResolve(executor, new GetMetadataResolver(
                ClientBuilder.newClient(config)));
    }

    private final class QueryResolver implements
            Callback.Resolver<MetricGroups> {
        private final RpcQueryBody request;
        private final Client client;

        private QueryResolver(RpcQueryBody request, Client client) {
            this.request = request;
            this.client = client;
        }

        @Override
        public MetricGroups resolve() throws Exception {
            final WebTarget target = client.target(url).path("rpc")
                    .path("query");
            return target.request().post(
                    Entity.entity(request, MediaType.APPLICATION_JSON),
                    MetricGroups.class);
        }
    }

    public Callback<MetricGroups> query(final RpcQueryBody request) {
        return ConcurrentCallback.newResolve(executor, new QueryResolver(
                request, ClientBuilder.newClient(config)));
    }
}
