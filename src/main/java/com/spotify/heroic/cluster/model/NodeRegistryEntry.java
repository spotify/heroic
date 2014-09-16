package com.spotify.heroic.cluster.model;

import java.net.URI;

import lombok.Data;

import com.spotify.heroic.cluster.ClusterNode;

@Data
public class NodeRegistryEntry {
    private final URI uri;
    private final ClusterNode clusterNode;
    private final NodeMetadata metadata;
}
