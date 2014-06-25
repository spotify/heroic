package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticAggregationCacheBackendReporter implements
        AggregationCacheBackendReporter {
    @SuppressWarnings("unused")
    private final SemanticMetricRegistry registry;
    @SuppressWarnings("unused")
    private final String context;
}
