package com.spotify.heroic.http.model.metadata;

import java.util.Map;
import java.util.Set;

import lombok.Data;

@Data
public class TagsResponse {
    private final Map<String, Set<String>> result;
    private final int sampleSize;
}
