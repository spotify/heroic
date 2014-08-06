package com.spotify.heroic.cluster;

import java.util.Map;
import java.util.UUID;

import lombok.Data;

@Data
public class ClusterNode {
    private final UUID id;
    private final String node;
    private final Map<String, String> tags;
}
