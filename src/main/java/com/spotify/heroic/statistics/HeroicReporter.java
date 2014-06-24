package com.spotify.heroic.statistics;

public interface HeroicReporter {
    MetricBackendManagerReporter newMetricBackendManager();

    MetadataBackendManagerReporter newMetadataBackendManager();

    AggregationCacheReporter newAggregationCache(String context);

    MetricBackendReporter newMetricBackend(String context);

    AggregationCacheBackendReporter newAggregationCacheBackend(String context);

    MetadataBackendReporter newMetadataBackend(String context);

    ConsumerReporter newConsumerReporter(String context);
}
