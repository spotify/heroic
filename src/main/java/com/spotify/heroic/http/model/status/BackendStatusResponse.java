package com.spotify.heroic.http.model.status;

import lombok.Data;

@Data
public class BackendStatusResponse {
    private final boolean ok;
    private final int available;
    private final int ready;
}
