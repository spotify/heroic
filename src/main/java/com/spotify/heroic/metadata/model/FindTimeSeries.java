package com.spotify.heroic.metadata.model;

import java.util.List;

import lombok.Getter;

import com.spotify.heroic.model.TimeSerie;

public class FindTimeSeries {
    @Getter
    private final List<TimeSerie> timeSeries;

    @Getter
    private final int size;

    public FindTimeSeries(List<TimeSerie> timeSeries, int size) {
        this.timeSeries = timeSeries;
        this.size = size;
    }
}