package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.BackendManagerReporter;
import com.spotify.heroic.statistics.BackendReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticHeroicReporter implements HeroicReporter {
    private final SemanticMetricRegistry registry;

    @Override
    public BackendManagerReporter newBackendManager(String context) {
        return new SemanticBackendManagerReporter(registry, context);
    }

    @Override
    public AggregationCacheReporter newAggregationCache(String context) {
        return new SemanticAggregationCacheReporter(registry, context);
    }

    @Override
    public BackendReporter newBackend(String context) {
        return new SemanticBackendReporter(registry, context);
    }

    @Override
    public AggregationCacheBackendReporter newAggregationCacheBackend(String context) {
        return new SemanticAggregationCacheBackendReporter(registry, context);
    }
}
