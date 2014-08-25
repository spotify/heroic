package com.spotify.heroic.metrics.model;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;

@RequiredArgsConstructor
@ToString(of = { "series", "range" })
public class FetchDataPoints {
    @Getter
    private final Series series;

    @Getter
    private final DateRange range;

    @RequiredArgsConstructor
    @ToString(of = { "datapoints", "series" })
    public static class Result {
        @Getter
        private final List<DataPoint> datapoints;

        @Getter
        private final Series series;
    }
}