package com.spotify.heroic.aggregation;

import java.util.List;

import com.spotify.heroic.common.Statistics;

import lombok.Data;

@Data
public class AggregationResult {
    private final List<AggregationData> result;
    private final Statistics statistics;
}