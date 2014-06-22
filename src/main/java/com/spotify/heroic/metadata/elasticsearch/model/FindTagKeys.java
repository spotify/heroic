package com.spotify.heroic.metadata.elasticsearch.model;

import java.util.Set;

import lombok.Getter;

public class FindTagKeys {
    @Getter
    private final Set<String> keys;

    @Getter
    private final int size;

    public FindTagKeys(Set<String> keys, int size) {
        this.keys = keys;
        this.size = size;
    }
}