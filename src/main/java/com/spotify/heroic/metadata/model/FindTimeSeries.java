package com.spotify.heroic.metadata.model;

import java.util.Set;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
public class FindTimeSeries {
    @Getter
    private final Set<TimeSerie> timeSeries;

    @Getter
    private final int size;
}