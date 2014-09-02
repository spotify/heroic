package com.spotify.heroic.http.metadata;

import lombok.Data;

@Data
public class MetadataDeleteSeriesResponse {
    private final int successful;
    private final int failed;
}
