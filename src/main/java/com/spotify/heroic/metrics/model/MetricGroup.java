package com.spotify.heroic.metrics.model;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.TimeSerie;

@Data
public final class MetricGroup {
    private final TimeSerie timeSerie;
    private final List<DataPoint> datapoints;
}