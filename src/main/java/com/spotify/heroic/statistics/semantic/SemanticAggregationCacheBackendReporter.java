package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticAggregationCacheBackendReporter implements AggregationCacheBackendReporter {
    private final SemanticMetricRegistry registry;
    private final String context;
}
