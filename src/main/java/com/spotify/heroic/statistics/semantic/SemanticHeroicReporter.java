package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticHeroicReporter implements HeroicReporter {
    private final SemanticMetricRegistry registry;

    @Override
    public MetricBackendManagerReporter newMetricBackendManager(String context) {
        return new SemanticMetricBackendManagerReporter(registry, context);
    }

    @Override
    public AggregationCacheReporter newAggregationCache(String context) {
        return new SemanticAggregationCacheReporter(registry, context);
    }

    @Override
    public MetricBackendReporter newMetricBackend(String context) {
        return new SemanticBackendReporter(registry, context);
    }

    @Override
    public AggregationCacheBackendReporter newAggregationCacheBackend(String context) {
        return new SemanticAggregationCacheBackendReporter(registry, context);
    }

    @Override
    public MetadataBackendManagerReporter newMetadataBackendManager(
            String context) {
        return new SemanticMetadataBackendManagerReporter(registry, context);
    }

    @Override
    public MetadataBackendReporter newMetadataBackend(String context) {
        return new SemanticMetadataBackendReporter(registry, context);
    }
}
