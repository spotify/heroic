package com.spotify.heroic.http.cluster;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Data;

import com.spotify.heroic.cluster.NodeCapability;

@Data
public class ClusterNodeStatus {
    private final String type;
    private final URI uri;
    private final UUID id;
    private final int version;
    private final Map<String, String> tags;
    private final Set<NodeCapability> capabilities;
}
