package com.spotify.heroic.http.model.metadata;

import java.util.Set;

import lombok.Data;

@Data
public class KeysResponse {
    private final Set<String> result;
    private final int sampleSize;
}
