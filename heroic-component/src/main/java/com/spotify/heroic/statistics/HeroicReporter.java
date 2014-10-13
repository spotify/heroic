package com.spotify.heroic.statistics;

public interface HeroicReporter {
    LocalMetricManagerReporter newLocalMetricBackendManager();

    ClusteredMetricManagerReporter newClusteredMetricBackendManager();

    LocalMetadataManagerReporter newLocalMetadataBackendManager();

    MetricBackendsReporter newMetricBackendsReporter();

    ClusteredMetadataManagerReporter newClusteredMetadataBackendManager();

    AggregationCacheReporter newAggregationCache();

    HttpClientManagerReporter newHttpClientManager();

    ConsumerReporter newConsumer(String id);
}
