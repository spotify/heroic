package com.spotify.heroic.statistics;

public interface HeroicReporter {
    MetricBackendManagerReporter newMetricBackendManager();

    MetadataBackendManagerReporter newMetadataBackendManager();

    AggregationCacheReporter newAggregationCache();

    ConsumerReporter newConsumer();

    AggregationCacheBackendReporter newAggregationCacheBackend();
}
