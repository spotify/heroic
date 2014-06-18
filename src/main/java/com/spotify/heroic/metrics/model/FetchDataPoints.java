package com.spotify.heroic.metrics.model;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.TimeSerie;

@RequiredArgsConstructor
@ToString(of = { "timeSerie", "range" })
public class FetchDataPoints {
    @Getter
    private final TimeSerie timeSerie;

    @Getter
    private final DateRange range;

    @RequiredArgsConstructor
    @ToString(of = { "datapoints", "timeSerie" })
    public static class Result {
        @Getter
        private final List<DataPoint> datapoints;

        @Getter
        private final TimeSerie timeSerie;
    }
}