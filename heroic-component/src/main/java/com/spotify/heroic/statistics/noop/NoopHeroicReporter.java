package com.spotify.heroic.statistics.noop;

import java.util.Map;
import java.util.Set;

import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.statistics.ClusteredMetricManagerReporter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.HttpClientManagerReporter;
import com.spotify.heroic.statistics.IngestionManagerReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.heroic.statistics.LocalMetricManagerReporter;
import com.spotify.heroic.statistics.MetricBackendGroupReporter;

public class NoopHeroicReporter implements HeroicReporter {
    @Override
    public LocalMetricManagerReporter newLocalMetricBackendManager() {
        return NoopLocalMetricManagerReporter.get();
    }

    @Override
    public ClusteredMetricManagerReporter newClusteredMetricBackendManager() {
        return NoopClusteredMetricManagerReporter.get();
    }

    @Override
    public LocalMetadataManagerReporter newLocalMetadataBackendManager() {
        return NoopLocalMetadataManagerReporter.get();
    }

    @Override
    public MetricBackendGroupReporter newMetricBackendsReporter() {
        return NoopMetricBackendsReporter.get();
    }

    @Override
    public ClusteredMetadataManagerReporter newClusteredMetadataBackendManager() {
        return NoopClusteredMetadataManagerReporter.get();
    }

    @Override
    public AggregationCacheReporter newAggregationCache() {
        return NoopAggregationCacheReporter.get();
    }

    @Override
    public HttpClientManagerReporter newHttpClientManager() {
        return NoopHttpClientManagerReporter.get();
    }

    @Override
    public ConsumerReporter newConsumer(String id) {
        return NoopConsumerReporter.get();
    }

    @Override
    public IngestionManagerReporter newIngestionManager() {
        return NoopIngestionManagerReporter.get();
    }

    @Override
    public void registerShards(Set<Map<String, String>> knownShards) {
    }

    private static final NoopHeroicReporter instance = new NoopHeroicReporter();

    public static NoopHeroicReporter get() {
        return instance;
    }
}
