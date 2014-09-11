package com.spotify.heroic.metadata.model;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import lombok.Getter;

public class FindTags {
    public static final FindTags EMPTY = new FindTags(
            new HashMap<String, Set<String>>(), 0);

    @Getter
    private final Map<String, Set<String>> tags;

    @Getter
    private final int size;

    public FindTags(Map<String, Set<String>> tags, int size) {
        this.tags = tags;
        this.size = size;
    }
}