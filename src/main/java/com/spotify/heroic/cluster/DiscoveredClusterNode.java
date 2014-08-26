package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.concurrent.Executor;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import lombok.Data;
import lombok.ToString;

import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.http.rpc.RpcMetadata;

@Data
@ToString(exclude = { "config", "executor" })
public class DiscoveredClusterNode {
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
            final RpcMetadata response = target.request()
                    .get(RpcMetadata.class);
            return new NodeMetadata(DiscoveredClusterNode.this,
                    response.getVersion(), response.getId(), response.getTags());
        }
    }

    public Callback<NodeMetadata> getMetadata() {
        return ConcurrentCallback.newResolve(executor, new GetMetadataResolver(
                ClientBuilder.newClient(config)));
    }
}
