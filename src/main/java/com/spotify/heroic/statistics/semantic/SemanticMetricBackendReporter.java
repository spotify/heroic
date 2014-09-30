package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.CallbackReporter.Context;
import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticMetricBackendReporter implements MetricBackendReporter {
    private static final String COMPONENT = "metric-backend";

    private final SemanticMetricRegistry registry;
    private final MetricId base;
    private final CallbackReporter writeBatch;

    public SemanticMetricBackendReporter(SemanticMetricRegistry registry, String id) {
        this.registry = registry;

        this.base = MetricId.build().tagged("component", COMPONENT, "id", id);

        this.writeBatch = new SemanticCallbackReporter(registry, base.tagged(
                "what", "write-batch", "unit", Units.WRITE));
    }

    @Override
    public Context reportWriteBatch() {
        return writeBatch.setup();
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return new SemanticThreadPoolReporter(registry, base);
    }
}