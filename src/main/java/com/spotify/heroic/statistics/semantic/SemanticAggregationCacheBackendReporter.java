package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticAggregationCacheBackendReporter implements AggregationCacheBackendReporter {
    private static final String COMPONENT = "aggregation-cache-backend";
    private final SemanticMetricRegistry registry;
    private final MetricId id;

    public SemanticAggregationCacheBackendReporter(SemanticMetricRegistry registry) {
        this.registry = registry;
        this.id = MetricId.build().tagged("component", COMPONENT);
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return new SemanticThreadPoolReporter(registry, id);
    }
}
