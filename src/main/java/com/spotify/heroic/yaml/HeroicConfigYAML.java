package com.spotify.heroic.yaml;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import com.spotify.heroic.backend.Backend;
import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.EventBackend;
import com.spotify.heroic.backend.ListBackendManager;
import com.spotify.heroic.backend.MetricBackend;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.AggregationCacheBackend;
import com.spotify.heroic.statistics.HeroicReporter;

public class HeroicConfigYAML {
    @Getter
    @Setter
    private List<Backend.YAML> backends;

    @Getter
    @Setter
    private AggregationCacheBackend.YAML cache;

    @Getter
    @Setter
    private long maxAggregationMagnitude = HeroicConfig.MAX_AGGREGATION_MAGNITUDE;

    @Getter
    @Setter
    private long maxQueriableDataPoints = HeroicConfig.MAX_QUERIABLE_DATA_POINTS;

    private List<Backend> setupBackends(String context, HeroicReporter reporter)
            throws ValidationException {
        List<Backend> backends = new ArrayList<Backend>();

        int i = 0;

        for (Backend.YAML backend : Utils.toList("backends", this.backends)) {
            backends.add(backend.build("backends[" + i++ + "]", reporter.newBackend(context)));
        }

        return backends;
    }

    private List<EventBackend> filterEventBackends(List<Backend> backends) {
        final List<EventBackend> eventBackends = new ArrayList<EventBackend>();

        for (final Backend backend : backends) {
            if (backend instanceof EventBackend)
                eventBackends.add((EventBackend) backend);
        }

        return eventBackends;
    }

    private List<MetricBackend> filterMetricBackends(List<Backend> backends) {
        final List<MetricBackend> metricBackends = new ArrayList<MetricBackend>();

        for (final Backend backend : backends) {
            if (backend instanceof MetricBackend)
                metricBackends.add((MetricBackend) backend);
        }

        return metricBackends;
    }

    public HeroicConfig build(HeroicReporter reporter)
            throws ValidationException {
        final List<Backend> backends = setupBackends("backends", reporter);

        final AggregationCache cache;

        if (this.cache == null) {
            cache = null;
        } else {
        	final AggregationCacheBackend backend = this.cache.build("cache", reporter.newAggregationCacheBackend(null));
            cache = new AggregationCache(reporter.newAggregationCache(null), backend);
        }

        final List<MetricBackend> metricBackends = filterMetricBackends(backends);
        final List<EventBackend> eventBackends = filterEventBackends(backends);

        final BackendManager backendManager = new ListBackendManager(metricBackends, eventBackends,
           cache, reporter.newBackendManager(null), maxAggregationMagnitude);

        return new HeroicConfig(backendManager, cache);
    }
}
