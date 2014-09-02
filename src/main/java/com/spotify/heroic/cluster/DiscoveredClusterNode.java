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
import com.spotify.heroic.http.rpc.RpcGetRequestResolver;
import com.spotify.heroic.http.rpc.RpcMetadata;

@Data
@ToString(exclude = { "config", "executor" })
public class DiscoveredClusterNode {
    private final URI url;
    private final ClientConfig config;
    private final Executor executor;

    private <R, T> Callback<T> get(Class<T> clazz, String endpoint) {
        final Client client = ClientBuilder.newClient(config);
        final WebTarget target = client.target(url).path("rpc").path(endpoint);
        return ConcurrentCallback.newResolve(executor,
                new RpcGetRequestResolver<T>(clazz, target));
    }

    public Callback<NodeMetadata> getMetadata() {
        final Callback.Transformer<RpcMetadata, NodeMetadata> transformer = new Callback.Transformer<RpcMetadata, NodeMetadata>() {
            @Override
            public NodeMetadata transform(RpcMetadata r) throws Exception {
                return new NodeMetadata(r.getVersion(), r.getId(), r.getTags(),
                        r.getCapabilities());
            }
        };

        return get(RpcMetadata.class, "metadata").transform(transformer);
    }
}
