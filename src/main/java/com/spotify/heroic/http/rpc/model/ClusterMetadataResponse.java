package com.spotify.heroic.http.rpc.model;

import java.util.Map;
import java.util.UUID;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

@Data
public class ClusterMetadataResponse {
    private final UUID id;
    private final Map<String, String> tags;

    @JsonCreator
    public static ClusterMetadataResponse create(
            @JsonProperty("id") UUID id,
            @JsonProperty("tags") Map<String, String> tags) {
        if (id == null)
            throw new IllegalArgumentException("'id' must be specified");

        if (tags == null || tags.isEmpty())
            throw new IllegalArgumentException("'tags' must be specified and non-empty");

        return new ClusterMetadataResponse(id, tags);
    }
}
