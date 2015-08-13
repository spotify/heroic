package com.spotify.heroic.metric.model;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.model.MetricType;
import com.spotify.heroic.model.TimeData;

@Data
public class TimeDataGroup {
    final MetricType type;
    final List<TimeData> data;
}