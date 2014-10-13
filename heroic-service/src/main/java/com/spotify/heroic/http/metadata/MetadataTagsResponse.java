package com.spotify.heroic.http.metadata;

import java.util.Map;
import java.util.Set;

import lombok.Data;

@Data
public class MetadataTagsResponse {
    private final Map<String, Set<String>> result;
    private final int sampleSize;
}
