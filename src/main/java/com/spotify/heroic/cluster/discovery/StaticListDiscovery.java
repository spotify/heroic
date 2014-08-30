package com.spotify.heroic.cluster.discovery;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import lombok.Data;
import lombok.RequiredArgsConstructor;

import org.glassfish.jersey.client.ClientConfig;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.cluster.ClusterDiscovery;
import com.spotify.heroic.cluster.DiscoveredClusterNode;
import com.spotify.heroic.yaml.ConfigContext;
import com.spotify.heroic.yaml.ValidationException;

@RequiredArgsConstructor
public class StaticListDiscovery implements ClusterDiscovery {
    @Data
    public static final class YAML implements ClusterDiscovery.YAML {
        public static final String TYPE = "!static-list-discovery";

        private int threadPoolSize = 100;
        private List<String> nodes = new ArrayList<String>();

        @Override
        public ClusterDiscovery build(ConfigContext context)
                throws ValidationException {
            final Executor executor = Executors
                    .newFixedThreadPool(threadPoolSize);

            final ClientConfig clientConfig = new ClientConfig();
            clientConfig.register(JacksonJsonProvider.class);
            final List<URI> nodeUris = parseURIs(context);

            return new StaticListDiscovery(nodeUris, clientConfig, executor);
        }

        private List<URI> parseURIs(ConfigContext context)
                throws ValidationException {
            final List<URI> nodeUris = new ArrayList<URI>();

            for (final ConfigContext.Entry<String> entry : context.iterate(
                    nodes, "nodes")) {
                nodeUris.add(parseUri(entry.getContext(), entry.getValue()));
            }

            return nodeUris;
        }

        private URI parseUri(ConfigContext context, final String n)
                throws ValidationException {
            try {
                return new URI(n);
            } catch (final URISyntaxException e) {
                throw new ValidationException(context, "Invalid URI", e);
            }
        }
    }

    private final List<URI> nodes;
    private final ClientConfig config;
    private final Executor executor;

    @Override
    public Callback<Collection<DiscoveredClusterNode>> getNodes() {
        final List<DiscoveredClusterNode> discovered = new ArrayList<>();

        for (final URI n : nodes) {
            discovered.add(new DiscoveredClusterNode(n, config, executor));
        }

        return new ResolvedCallback<Collection<DiscoveredClusterNode>>(
                discovered);
    }
}
