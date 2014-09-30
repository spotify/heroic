package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Histogram;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticAggregationCacheReporter implements
AggregationCacheReporter {
    private static final String COMPONENT = "aggregation-cache";

    private final CallbackReporter get;
    private final CallbackReporter put;
    private final Histogram getMiss;

    private final SemanticMetricRegistry registry;

    public SemanticAggregationCacheReporter(SemanticMetricRegistry registry) {
        this.registry = registry;

        final MetricId id = MetricId.build().tagged("component", COMPONENT);

        this.get = new SemanticCallbackReporter(registry, id.tagged("what",
                "get", "unit", Units.READ));
        this.put = new SemanticCallbackReporter(registry, id.tagged("what",
                "put", "unit", Units.WRITE));
        getMiss = registry.histogram(id.tagged("what", "get-miss", "unit",
                Units.MISS));
    }

    @Override
    public CallbackReporter.Context reportGet() {
        return get.setup();
    }

    @Override
    public CallbackReporter.Context reportPut() {
        return put.setup();
    }

    @Override
    public void reportGetMiss(int size) {
        getMiss.update(size);
    }

    @Override
    public AggregationCacheBackendReporter newAggregationCacheBackend() {
        return new SemanticAggregationCacheBackendReporter(registry);
    }
}
