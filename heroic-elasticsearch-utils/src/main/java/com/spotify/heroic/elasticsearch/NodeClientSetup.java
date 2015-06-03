package com.spotify.heroic.elasticsearch;

import java.net.InetAddress;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Optional;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;

public class NodeClientSetup implements ClientSetup {
    public static final String DEFAULT_CLUSTER_NAME = "elasticsearch";

    private final String clusterName;
    private final String[] seeds;

    private final Object $lock = new Object();
    private final AtomicReference<Node> node = new AtomicReference<>();

    @JsonCreator
    public NodeClientSetup(String clusterName, List<String> seeds) {
        this.clusterName = Optional.fromNullable(clusterName).or(DEFAULT_CLUSTER_NAME);
        this.seeds = seedsToDiscovery(seeds);
    }

    @Override
    public Client setup() throws Exception {
        synchronized ($lock) {
            if (node.get() != null)
                throw new IllegalStateException("already started");

            final Settings settings = ImmutableSettings.builder()
                    .put("node.name", InetAddress.getLocalHost().getHostName())
                    .put("discovery.zen.ping.multicast.enabled", false)
                    .putArray("discovery.zen.ping.unicast.hosts", seeds).build();

            final Node node = NodeBuilder.nodeBuilder().settings(settings).client(true).clusterName(clusterName).node();

            this.node.set(node);
            return node.client();
        }
    }

    @Override
    public void stop() throws Exception {
        synchronized ($lock) {
            final Node node = this.node.getAndSet(null);

            if (node == null)
                throw new IllegalStateException("not started");

            node.stop();
        }
    }

    private String[] seedsToDiscovery(List<String> seeds) {
        return seeds.toArray(new String[0]);
    }
}
