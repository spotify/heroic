package com.spotify.heroic.query;

import java.util.Set;

import lombok.Getter;

public class KeysResponse {
    @Getter
    private final Set<String> result;

    public KeysResponse(final Set<String> result) {
        this.result = result;
    }
}
