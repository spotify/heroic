package com.spotify.heroic.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.spotify.heroic.cluster.model.NodeMetadata;

public class NodeRegistry {
    private final List<ClusterNode> nodes;

    public NodeRegistry(Collection<NodeMetadata> metadata) {
        this.nodes = makeNodes(metadata);
    }

    private List<ClusterNode> makeNodes(Collection<NodeMetadata> metadata) {
        final List<ClusterNode> nodes = new ArrayList<ClusterNode>();

        for (final NodeMetadata m : metadata) {
            nodes.add(makeNode(m));
        }

        return nodes;
    }

    private ClusterNode makeNode(NodeMetadata m) {
        return new ClusterNode(m.getId(), m.getNode(), m.getTags());
    }

    public ClusterNode findNode(Map<String, String> tags) {
        final List<ClusterNode> alternatives = new ArrayList<>();

        for (final ClusterNode n : nodes) {
            if (!tagsMatch(n.getTags(), tags))
                continue;
        }

        return null;
    }

    private boolean tagsMatch(Map<String, String> node, Map<String, String> query) {
        for (Map.Entry<String, String> entry : node.entrySet()) {
            final String entryValue = entry.getValue();
            final String queryValue = query.get(entry.getKey());

            if (queryValue == null)
                return entryValue == null;
        }

        return false;
    }
}