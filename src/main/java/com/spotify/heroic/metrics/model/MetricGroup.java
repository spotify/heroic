package com.spotify.heroic.metrics.model;

import java.util.List;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;

@ToString(of={"timeSerie", "datapoints"})
@RequiredArgsConstructor
public final class MetricGroup {
    @Getter
    private final TimeSerie timeSerie;

    @Getter
    private final List<DataPoint> datapoints;
}