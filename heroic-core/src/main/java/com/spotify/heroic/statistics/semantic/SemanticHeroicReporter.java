package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.statistics.ClusteredMetricManagerReporter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.HttpClientManagerReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.statistics.LocalMetricManagerReporter;
import com.spotify.heroic.statistics.MetricBackendsReporter;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticHeroicReporter implements HeroicReporter {
    private final SemanticMetricRegistry registry;

    @Override
    public LocalMetricManagerReporter newLocalMetricBackendManager() {
        return new SemanticLocalMetricManagerReporter(registry);
    }

    @Override
    public LocalMetadataManagerReporter newLocalMetadataBackendManager() {
        return new SemanticMetadataManagerReporter(registry);
    }

    @Override
    public AggregationCacheReporter newAggregationCache() {
        return new SemanticAggregationCacheReporter(registry);
    }

    @Override
    public ConsumerReporter newConsumer(String id) {
        return new SemanticConsumerReporter(registry, id);
    }

    @Override
    public HttpClientManagerReporter newHttpClientManager() {
        return new SemanticHttpClientManagerReporter(registry);
    }

    @Override
    public ClusteredMetricManagerReporter newClusteredMetricBackendManager() {
        return new SemanticClusteredMetricManagerReporter(registry);
    }

    @Override
    public ClusteredMetadataManagerReporter newClusteredMetadataBackendManager() {
        return new SemanticClusteredMetadataManagerReporter(registry);
    }

    @Override
    public MetricBackendsReporter newMetricBackendsReporter() {
        return new SemanticMetricBackendsReporter(registry);
    }
}
