package com.spotify.heroic.http.write;

import lombok.Data;

@Data
public class WriteMetricsResponse {
    private final int successful;
    private final int failed;
    private final int cancelled;
}
