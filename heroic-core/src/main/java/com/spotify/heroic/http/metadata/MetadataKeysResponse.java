package com.spotify.heroic.http.metadata;

import java.util.Set;

import lombok.Data;

@Data
public class MetadataKeysResponse {
    private final Set<String> result;
    private final int sampleSize;
}
