package com.spotify.heroic.cluster;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.async.CancelledCallback;
import com.spotify.heroic.async.FailedCallback;
import com.spotify.heroic.async.ResolvedCallback;
import com.spotify.heroic.yaml.Utils;
import com.spotify.heroic.yaml.ValidationException;

@RequiredArgsConstructor
public class ClusterManager {
    public static final class YAML {
        public ClusterDiscovery.YAML discovery;
        public Map<String, String> tags = new HashMap<String, String>();

        public ClusterManager build(String context) throws ValidationException {
            Utils.notNull(context + ".discovery", discovery);
            Utils.notEmpty(context + ".tags", tags);
            final ClusterDiscovery discovery = this.discovery.build(context + ".discovery");
            return new ClusterManager(discovery, UUID.randomUUID(), tags);
        }
    }

    private static final CancelReason REGISTRY_NOT_READY = new CancelReason("registry is not ready yet");

    private final ClusterDiscovery discovery;

    @Getter
    private final UUID nodeId;
    @Getter
    private final Map<String, String> nodeTags;

    private final AtomicReference<NodeRegistry> registry = new AtomicReference<>();

    public final Callback<ClusterNode> findNode(final Map<String, String> tags) {
        final NodeRegistry registry = this.registry.get();

        if (registry == null)
            return new CancelledCallback<>(REGISTRY_NOT_READY);

        final ClusterNode node = registry.findNode(tags);

        if (node == null)
            return new FailedCallback<>(new Exception("node not found"));

        return new ResolvedCallback<>(node);
    }

    public void refresh() throws Exception {
        this.registry.set(new NodeRegistry(discovery.getNodes().get()));
    }
}
