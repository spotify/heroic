package com.spotify.heroic.statistics;

public interface HeroicReporter {
    MetricBackendManagerReporter newMetricBackendManager(String context);

    MetadataBackendManagerReporter newMetadataBackendManager(String context);

    AggregationCacheReporter newAggregationCache(String context);

    MetricBackendReporter newMetricBackend(String context);

    AggregationCacheBackendReporter newAggregationCacheBackend(String context);

    MetadataBackendReporter newMetadataBackend(String context);

    ConsumerReporter newConsumerReporter(String context);
}
