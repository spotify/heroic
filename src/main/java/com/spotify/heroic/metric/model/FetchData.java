package com.spotify.heroic.metric.model;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Series;

@Data
public class FetchData {
    private final Series series;
    private final List<DataPoint> datapoints;
}