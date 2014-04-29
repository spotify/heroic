package com.spotify.heroic.yaml;

import java.util.ArrayList;
import java.util.List;

import lombok.Getter;
import lombok.Setter;

import com.codahale.metrics.MetricRegistry;
import com.spotify.heroic.backend.Backend;
import com.spotify.heroic.backend.BackendManager;
import com.spotify.heroic.backend.ListBackendManager;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.AggregationCacheBackend;

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

    private List<Backend> setupBackends(String context, MetricRegistry registry)
            throws ValidationException {
        List<Backend> backends = new ArrayList<Backend>();

        int i = 0;

        for (Backend.YAML backend : Utils.toList("backends", this.backends)) {
            backends.add(backend.build("backends[" + i++ + "]", registry));
        }

        return backends;
    }

    public HeroicConfig build(MetricRegistry registry)
            throws ValidationException {
        final List<Backend> backends = setupBackends("backends", registry);

        final AggregationCache cache;

        if (this.cache == null) {
            cache = null;
        } else {
            cache = new AggregationCache(registry, this.cache.build("cache"));
        }

        final BackendManager backendManager = new ListBackendManager(backends,
                registry, maxAggregationMagnitude, maxQueriableDataPoints,
                cache);

        return new HeroicConfig(backendManager, cache);
    }
}
