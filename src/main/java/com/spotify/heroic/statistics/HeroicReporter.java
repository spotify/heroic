package com.spotify.heroic.statistics;

public interface HeroicReporter {
    MetricManagerReporter newMetricBackendManager();

    MetadataManagerReporter newMetadataBackendManager();

    AggregationCacheReporter newAggregationCache();

    HttpClientManagerReporter newHttpClientManager();

    ConsumerReporter newConsumer(String id);
}
