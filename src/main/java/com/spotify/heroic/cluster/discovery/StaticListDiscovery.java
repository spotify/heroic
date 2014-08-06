package com.spotify.heroic.cluster.discovery;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.glassfish.jersey.client.ClientConfig;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.ConcurrentCallback;
import com.spotify.heroic.cluster.ClusterDiscovery;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.http.model.ClusterMetadataResponse;
import com.spotify.heroic.yaml.ValidationException;

@Slf4j
@RequiredArgsConstructor
public class StaticListDiscovery implements ClusterDiscovery {
    public static final class YAML implements ClusterDiscovery.YAML {
        public static final String TYPE = "!static-list-discovery";

        public List<String> nodes = new ArrayList<String>();

        @Override
        public ClusterDiscovery build(String context)
                throws ValidationException {
            final Executor executor = Executors.newFixedThreadPool(10);

            final ClientConfig clientConfig = new ClientConfig();
            clientConfig.register(JacksonJsonProvider.class);

            return new StaticListDiscovery(nodes, executor, clientConfig);
        }
    }

    @Data
    public static final class NodeResponse {
        private final String node;
        private final ClusterMetadataResponse response;
    }

    private final List<String> nodes;
    private final Executor executor;
    private final ClientConfig config;

    @Override
    public Callback<Collection<NodeMetadata>> getNodes() {
        final List<Callback<NodeResponse>> callbacks = new ArrayList<Callback<NodeResponse>>();

        for (final String node : nodes) {
            final Client client = ClientBuilder.newClient(config);

            final Callback<NodeResponse> resolve = ConcurrentCallback.newResolve(executor, new Callback.Resolver<NodeResponse>() {
                @Override
                public NodeResponse resolve() throws Exception {
                    final WebTarget target = client.target(node).path("rpc").path("_meta");
                    final ClusterMetadataResponse response = target.request().get(ClusterMetadataResponse.class);
                    return new NodeResponse(node, response);
                }
            });

            callbacks.add(resolve);
        }

        return ConcurrentCallback.newReduce(callbacks, new Callback.Reducer<NodeResponse, Collection<NodeMetadata>>() {
            @Override
            public Collection<NodeMetadata> resolved(
                    Collection<NodeResponse> results, Collection<Exception> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                for (final Exception error : errors) {
                    log.error("Failed to get metadata for node", error);
                }

                final List<NodeMetadata> result = new ArrayList<>();

                for (final NodeResponse response : results) {
                    final NodeMetadata node = convertResponse(response);
                    log.info("refreshed node: {}", node);
                    result.add(node);
                }

                return result;
            }

            private NodeMetadata convertResponse(NodeResponse r) {
                final ClusterMetadataResponse response = r.getResponse();
                return new NodeMetadata(r.getNode(), response.getId(), response.getTags());
            }
        });
    }
}
