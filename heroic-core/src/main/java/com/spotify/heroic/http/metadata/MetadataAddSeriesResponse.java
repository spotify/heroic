package com.spotify.heroic.http.metadata;

import java.util.List;

import lombok.Data;

@Data
public class MetadataAddSeriesResponse {
    private final List<Long> times;
}