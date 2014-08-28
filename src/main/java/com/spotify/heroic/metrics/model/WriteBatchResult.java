package com.spotify.heroic.metrics.model;

import lombok.Data;

@Data
public class WriteBatchResult {
    private final boolean ok;
    private final int requests;
}
