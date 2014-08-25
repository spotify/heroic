package com.spotify.heroic.metadata.model;

import java.util.Set;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
public class FindTimeSeries {
    @Getter
    private final Set<Series> series;

    @Getter
    private final int size;
}