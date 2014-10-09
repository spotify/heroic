package com.spotify.heroic.metric.model;

import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.spotify.heroic.model.Series;

@Data
public class FindTimeSeriesGroups {
    private final Map<Map<String, String>, Set<Series>> groups;
}