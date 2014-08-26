package com.spotify.heroic.http.rpc;

import java.util.Map;
import java.util.UUID;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class RpcMetadata {
    private static final int DEFAULT_VERSION = 0;

    private final int version;
    private final UUID id;
    private final Map<String, String> tags;

    @JsonCreator
    public static RpcMetadata create(@JsonProperty("version") Integer version,
            @JsonProperty("id") UUID id,
            @JsonProperty("tags") Map<String, String> tags) {
        if (version == null)
            version = DEFAULT_VERSION;

        if (id == null)
            throw new IllegalArgumentException("'id' must be specified");

        if (tags == null || tags.isEmpty())
            throw new IllegalArgumentException(
                    "'tags' must be specified and non-empty");

        return new RpcMetadata(version, id, tags);
    }
}
