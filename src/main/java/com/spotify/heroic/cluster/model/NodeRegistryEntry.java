package com.spotify.heroic.cluster.model;

import lombok.Data;

import com.spotify.heroic.cluster.ClusterNode;

@Data
public class NodeRegistryEntry {
    private final ClusterNode clusterNode;
    private final NodeMetadata metadata;
}
