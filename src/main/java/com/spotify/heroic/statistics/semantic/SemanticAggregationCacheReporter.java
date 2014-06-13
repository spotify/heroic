package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticAggregationCacheReporter implements AggregationCacheReporter {
    private final SemanticMetricRegistry registry;
    private final String context;
}
