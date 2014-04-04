package com.spotify.heroic.query;

import java.util.Set;

import lombok.Getter;

public class KeysResponse {
    @Getter
    private final Set<String> result;

    @Getter
    private final int sampleSize;

    public KeysResponse(final Set<String> result, int sampleSize) {
        this.result = result;
        this.sampleSize = sampleSize;
    }
}
