package com.spotify.heroic.statistics;

public interface HeroicReporter {
    MetricBackendManagerReporter newMetricBackendManager();

    MetadataBackendManagerReporter newMetadataBackendManager();

    AggregationCacheReporter newAggregationCache();

    HttpClientManagerReporter newHttpClientManager();

    ConsumerReporter newConsumer(String id);
}
