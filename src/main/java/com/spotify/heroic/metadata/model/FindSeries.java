package com.spotify.heroic.metadata.model;

import java.util.HashSet;
import java.util.Set;

import lombok.Data;

import com.spotify.heroic.model.Series;

@Data
public class FindSeries {
    public static final FindSeries EMPTY = new FindSeries(
            new HashSet<Series>(), 0, 0);

    private final Set<Series> series;
    private final int size;
    private final int duplicates;
}