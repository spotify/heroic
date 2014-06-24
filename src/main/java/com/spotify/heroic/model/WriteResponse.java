package com.spotify.heroic.model;

import lombok.Data;

@Data
public class WriteResponse {
    private final int failed;
    private final int cancelled;

    public WriteResponse merge(final WriteResponse other) {
        return new WriteResponse(this.failed + other.failed, this.cancelled
                + other.cancelled);
    }
}
