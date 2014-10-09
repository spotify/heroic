package com.spotify.heroic.http.rpc;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.cluster.NodeCapability;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class RpcMetadata {
    public static final int DEFAULT_VERSION = 0;

    public static final Set<NodeCapability> DEFAULT_CAPABILITIES = new HashSet<>();
    static {
        DEFAULT_CAPABILITIES.add(NodeCapability.QUERY);
    }

    private final int version;
    private final UUID id;
    private final Map<String, String> tags;
    private final Set<NodeCapability> capabilities;

    @JsonCreator
    public static RpcMetadata create(@JsonProperty("version") Integer version, @JsonProperty("id") UUID id,
            @JsonProperty(value = "tags", required = false) Map<String, String> tags,
            @JsonProperty(value = "capabilities", required = false) Set<NodeCapability> capabilities) {
        if (version == null)
            version = DEFAULT_VERSION;

        if (capabilities == null)
            capabilities = DEFAULT_CAPABILITIES;

        if (id == null)
            throw new IllegalArgumentException("'id' must be specified");

        return new RpcMetadata(version, id, tags, capabilities);
    }
}
