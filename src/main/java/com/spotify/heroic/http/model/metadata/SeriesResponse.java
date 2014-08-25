package com.spotify.heroic.http.model.metadata;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.Series;

@Data
public class SeriesResponse {
    private final List<Series> result;
    private final int sampleSize;
}
