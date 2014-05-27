package com.spotify.heroic.http.model;

import java.util.Map;
import java.util.Set;

import lombok.Getter;

public class TagsResponse {
    @Getter
    private final Map<String, Set<String>> result;

    @Getter
    private final int sampleSize;

    public TagsResponse(final Map<String, Set<String>> result, int sampleSize) {
        this.result = result;
        this.sampleSize = sampleSize;
    }
}
