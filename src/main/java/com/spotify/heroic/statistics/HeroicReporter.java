package com.spotify.heroic.statistics;

import com.spotify.heroic.yaml.ConfigContext;

public interface HeroicReporter {
    MetricBackendManagerReporter newMetricBackendManager();

    MetadataBackendManagerReporter newMetadataBackendManager();

    AggregationCacheReporter newAggregationCache(ConfigContext context);

    ConsumerReporter newConsumerReporter(ConfigContext context);
}
