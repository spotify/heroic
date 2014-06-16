package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Histogram;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticAggregationCacheReporter implements AggregationCacheReporter {
    private final CallbackReporter getReporter;
    private final CallbackReporter putReporter;
    private final Histogram getMisses;

    public SemanticAggregationCacheReporter(SemanticMetricRegistry registry, String context) {
        final MetricId id = MetricId.build("aggregation-cache").tagged("context", context);
        final MetricId backend = id.resolve("request");
        getReporter = new SemanticCallbackReporter(registry, backend.tagged("operation", "get"));
        putReporter = new SemanticCallbackReporter(registry, backend.tagged("operation", "get"));
        getMisses = registry.histogram(id.resolve("get-misses"));
    }

    @Override
    public CallbackReporter.Context reportGet() {
        return getReporter.setup();
    }

    @Override
    public CallbackReporter.Context reportPut() {
        return putReporter.setup();
    }

    @Override
    public void reportGetMisses(int size) {
        getMisses.update(size);
    }
}
