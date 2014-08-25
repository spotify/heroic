package com.spotify.heroic.metrics.model;

import java.util.Map;
import java.util.Set;

import lombok.Data;
import lombok.Getter;

import com.spotify.heroic.model.Series;

@Data
public class FindTimeSeriesGroups {
    @Getter
    private final Map<Series, Set<Series>> groups;
}