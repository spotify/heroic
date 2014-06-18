package com.spotify.heroic.metadata.model;

import java.util.Map;
import java.util.Set;

import lombok.Getter;

public class FindTags {
    @Getter
    private final Map<String, Set<String>> tags;

    @Getter
    private final int size;

    public FindTags(Map<String, Set<String>> tags, int size) {
        this.tags = tags;
        this.size = size;
    }
}