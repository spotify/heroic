package com.spotify.heroic.aggregation;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.common.Statistics;

@Data
public class AggregationResult {
    private final List<AggregationData> result;
    private final Statistics.Aggregator statistics;
}