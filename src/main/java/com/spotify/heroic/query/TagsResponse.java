package com.spotify.heroic.query;

import java.util.Map;
import java.util.Set;

import lombok.Getter;

public class TagsResponse {
    @Getter
    private final Map<String, Set<String>> result;

    @Getter
    private final Set<String> metrics;

    public TagsResponse(final Map<String, Set<String>> result,
            final Set<String> metrics) {
        this.result = result;
        this.metrics = metrics;
    }
}
