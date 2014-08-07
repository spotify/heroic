package com.spotify.heroic.cluster;

import java.net.URI;
import java.util.concurrent.Executor;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import lombok.Data;

import org.glassfish.jersey.client.ClientConfig;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.http.rpc.model.ClusterMetadataResponse;

@Data
public class ClusterNode {
    private final URI url;
    private final Executor executor;
    private final ClientConfig config;

    public Callback<NodeMetadata> getMetadata() {
        final Client client = ClientBuilder.newClient(config);

        return ConcurrentCallback.newResolve(
                executor, new Callback.Resolver<NodeMetadata>() {
                    @Override
                    public NodeMetadata resolve() throws Exception {
                        final WebTarget target = client.target(url).path("rpc")
                                .path("metadata");
                        final ClusterMetadataResponse response = target
                                .request().get(ClusterMetadataResponse.class);
                        return new NodeMetadata(response
                                .getId(), response.getTags());
                    }
                });
    }
}
