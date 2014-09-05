package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.BackendReporter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticHeroicReporter implements HeroicReporter {
    private final SemanticMetricRegistry registry;

    @Override
    public MetricBackendManagerReporter newMetricBackendManager() {
        return new SemanticMetricBackendManagerReporter(registry);
    }

    @Override
    public MetadataBackendManagerReporter newMetadataBackendManager() {
        return new SemanticMetadataBackendManagerReporter(registry);
    }

    @Override
    public AggregationCacheReporter newAggregationCache() {
        return new SemanticAggregationCacheReporter(registry);
    }

    @Override
    public ConsumerReporter newConsumer() {
        return new SemanticConsumerReporter(registry);
    }

    @Override
    public MetadataBackendReporter newMetadataBackend() {
        return new SemanticMetadataBackendReporter(registry);
    }

    @Override
    public BackendReporter newBackend() {
        return new SemanticBackendReporter(registry);
    }

    @Override
    public AggregationCacheBackendReporter newAggregationCacheBackend() {
        return new SemanticAggregationCacheBackendReporter(registry);
    }
}
