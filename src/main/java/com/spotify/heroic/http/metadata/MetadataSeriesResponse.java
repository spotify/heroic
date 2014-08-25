package com.spotify.heroic.http.metadata;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.Series;

@Data
public class MetadataSeriesResponse {
    private final List<Series> result;
    private final int sampleSize;
}
