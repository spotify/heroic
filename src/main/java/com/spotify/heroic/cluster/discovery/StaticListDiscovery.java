package com.spotify.heroic.cluster.discovery;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.RequiredArgsConstructor;

import org.glassfish.jersey.client.ClientConfig;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cluster.ClusterDiscovery;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.yaml.ValidationException;

@RequiredArgsConstructor
public class StaticListDiscovery implements ClusterDiscovery {
    public static final class YAML implements ClusterDiscovery.YAML {
        public static final String TYPE = "!static-list-discovery";

        public int threadPoolSize = 100;

        public List<String> nodes = new ArrayList<String>();

        @Override
        public ClusterDiscovery build(String context)
                throws ValidationException {
            final Executor executor = Executors
                    .newFixedThreadPool(threadPoolSize);

            final ClientConfig clientConfig = new ClientConfig();
            clientConfig.register(JacksonJsonProvider.class);
            final List<URI> nodeUris = parseURIs(context + ".nodes");

            return new StaticListDiscovery(nodeUris, executor, clientConfig);
        }

        private List<URI> parseURIs(String context) throws ValidationException {
            final List<URI> nodeUris = new ArrayList<URI>();
            for (final String n : nodes) {
                try {
                    nodeUris.add(new URI(n));
                } catch (final URISyntaxException e) {
                    throw new ValidationException(context + ": Invalid URI: "
                            + n, e);
                }
            }

            return nodeUris;
        }
    }

    private final List<URI> nodes;
    private final Executor executor;
    private final ClientConfig config;

    @Override
    public Callback<Collection<ClusterNode>> getNodes() {
        final List<ClusterNode> clusterNodes = new ArrayList<>();

        for (final URI n : nodes) {
            clusterNodes.add(new ClusterNode(n, config, executor));
        }

        return new ResolvedCallback<Collection<ClusterNode>>(clusterNodes);
    }
}
