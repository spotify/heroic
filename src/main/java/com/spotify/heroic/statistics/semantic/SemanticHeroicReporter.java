package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;
import com.spotify.heroic.yaml.ConfigContext;
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
    public AggregationCacheReporter newAggregationCache(ConfigContext context) {
        return new SemanticAggregationCacheReporter(registry, context);
    }

    @Override
    public ConsumerReporter newConsumerReporter(ConfigContext context) {
        return new SemanticConsumerReporter(registry, context);
    }
}
