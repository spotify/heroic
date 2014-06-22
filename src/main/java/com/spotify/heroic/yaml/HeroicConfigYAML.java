package com.spotify.heroic.yaml;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.AggregationCacheBackend;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.metadata.InMemoryMetadataBackend;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.statistics.HeroicReporter;

public class HeroicConfigYAML {
    @Getter
    @Setter
    private List<MetricBackend.YAML> backends;

    @Getter
    @Setter
    private List<MetadataBackend.YAML> metadataBackends;

    @Getter
    @Setter
    private List<Consumer.YAML> consumers;

    @Getter
    @Setter
    private AggregationCacheBackend.YAML cache;

    @Getter
    @Setter
    private long maxAggregationMagnitude = HeroicConfig.MAX_AGGREGATION_MAGNITUDE;

    @Getter
    @Setter
    private long maxQueriableDataPoints = HeroicConfig.MAX_QUERIABLE_DATA_POINTS;

    private List<MetricBackend> setupMetricBackends(String context, HeroicReporter reporter)
            throws ValidationException {
        List<MetricBackend> backends = new ArrayList<MetricBackend>();

        int i = 0;

        for (MetricBackend.YAML backend : Utils.toList("backends", this.backends)) {
            final String c = context + "[" + i++ + "]";
            backends.add(backend.build(c, reporter.newMetricBackend(c)));
        }

        return backends;
    }

    private List<MetadataBackend> setupMetadataBackends(String context, HeroicReporter reporter)
            throws ValidationException {
        List<MetadataBackend> backends = new ArrayList<MetadataBackend>();

        int i = 0;

        for (MetadataBackend.YAML backend : Utils.toList("metadataBackends", this.metadataBackends)) {
            final String c = context + "[" + i++ + "]";
            backends.add(backend.build(c, reporter.newMetadataBackend(c)));
        }

        return backends;
    }

    private List<Consumer> setupConsumers(String context,
            HeroicReporter reporter) throws ValidationException {

        List<Consumer> consumers = new ArrayList<Consumer>();

        int i = 0;

        for (Consumer.YAML consumer : Utils.toList("consumers",
                this.consumers)) {
            final String c = context + "[" + i++ + "]";
            consumers.add(consumer.build(c, reporter.newConsumerReporter(c)));
        }

        return consumers;
    }

    public HeroicConfig build(HeroicReporter reporter)
            throws ValidationException {
        final List<MetricBackend> metricBackends = setupMetricBackends("backends", reporter);
        final List<MetadataBackend> metadataBackends = setupMetadataBackends("metadataBackends", reporter);
        final List<Consumer> consumers = setupConsumers("consumers", reporter);

        if (metadataBackends.isEmpty()) {
            metadataBackends.add(new InMemoryMetadataBackend(reporter.newMetadataBackend(null)));
        }

        final AggregationCache cache;

        if (this.cache == null) {
            cache = null;
        } else {
            final AggregationCacheBackend backend = this.cache.build("cache", reporter.newAggregationCacheBackend(null));
            cache = new AggregationCache(reporter.newAggregationCache(null), backend);
        }

        return new HeroicConfig(metricBackends, metadataBackends, consumers,
                cache,
                maxAggregationMagnitude);
    }
}
