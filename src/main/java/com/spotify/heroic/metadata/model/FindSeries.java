package com.spotify.heroic.metadata.model;

import java.util.Set;

import lombok.Data;

import com.spotify.heroic.model.Series;

@Data
public class FindSeries {
    private final Set<Series> series;
    private final int size;
    private final int duplicates;
}