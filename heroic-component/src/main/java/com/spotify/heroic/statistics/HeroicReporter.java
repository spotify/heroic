package com.spotify.heroic.statistics;

import java.util.Map;
import java.util.Set;

public interface HeroicReporter {
    LocalMetricManagerReporter newLocalMetricBackendManager();

    ClusteredMetricManagerReporter newClusteredMetricBackendManager();

    LocalMetadataManagerReporter newLocalMetadataBackendManager();

    MetricBackendGroupReporter newMetricBackendsReporter();

    ClusteredMetadataManagerReporter newClusteredMetadataBackendManager();

    AggregationCacheReporter newAggregationCache();

    HttpClientManagerReporter newHttpClientManager();

    ConsumerReporter newConsumer(String id);

    IngestionManagerReporter newIngestionManager();

    void registerShards(Set<Map<String, String>> knownShards);
}
