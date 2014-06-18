package com.spotify.heroic.yaml;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.AggregationCacheBackend;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.MetricBackendManager;
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
            backends.add(backend.build("backends[" + i++ + "]", reporter.newMetricBackend(context)));
        }

        return backends;
    }

    private List<MetadataBackend> setupMetadataBackends(String context, HeroicReporter reporter)
            throws ValidationException {
        List<MetadataBackend> backends = new ArrayList<MetadataBackend>();

        int i = 0;

        for (MetadataBackend.YAML backend : Utils.toList("metadataBackends", this.metadataBackends)) {
            backends.add(backend.build("metadataBackends[" + i++ + "]", reporter.newMetadataBackend(context)));
        }

        return backends;
    }

    public HeroicConfig build(HeroicReporter reporter)
            throws ValidationException {
        final List<MetricBackend> metricBackends = setupMetricBackends("backends", reporter);
        final List<MetadataBackend> metadataBackends = setupMetadataBackends("metadataBackends", reporter);

        final AggregationCache cache;

        if (this.cache == null) {
            cache = null;
        } else {
        	final AggregationCacheBackend backend = this.cache.build("cache", reporter.newAggregationCacheBackend(null));
            cache = new AggregationCache(reporter.newAggregationCache(null), backend);
        }

        final MetricBackendManager metrics = new MetricBackendManager(metricBackends, reporter.newMetricBackendManager(null), maxAggregationMagnitude);
        final MetadataBackendManager metadata = new MetadataBackendManager(metadataBackends, reporter.newMetadataBackendManager(null));
        return new HeroicConfig(metrics, metadata, cache);
    }
}
