package com.spotify.heroic.metadata.model;

import java.util.Set;

import lombok.Getter;

public class FindKeys {
    @Getter
    private final Set<String> keys;

    @Getter
    private final int size;

    public FindKeys(Set<String> keys, int size) {
        this.keys = keys;
        this.size = size;
    }
}