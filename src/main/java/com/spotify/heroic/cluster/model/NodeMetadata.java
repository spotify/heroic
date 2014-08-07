package com.spotify.heroic.cluster.model;

import java.util.Map;
import java.util.UUID;

import lombok.Data;

@Data
public class NodeMetadata {
    private final UUID id;
    private final Map<String, String> tags;
}
