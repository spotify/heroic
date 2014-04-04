package com.spotify.heroic.query;

import java.util.List;

import lombok.Getter;

public class KeysResponse {
    @Getter
    private final List<String> result;

    public KeysResponse(final List<String> result) {
        this.result = result;
    }
}
