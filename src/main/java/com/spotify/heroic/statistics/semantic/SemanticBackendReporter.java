package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.BackendReporter;
import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.CallbackReporter.Context;
import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticBackendReporter implements BackendReporter {
    private static final String COMPONENT = "metric-backend";

    private final SemanticMetricRegistry registry;
    private final MetricId id;
    private final CallbackReporter writeBatch;

    public SemanticBackendReporter(SemanticMetricRegistry registry) {
        this.registry = registry;

        this.id = MetricId.build().tagged("component", COMPONENT);

        this.writeBatch = new SemanticCallbackReporter(registry, id.tagged(
                "what", "write-batch", "unit", Units.WRITE));
    }

    @Override
    public Context reportWriteBatch() {
        return writeBatch.setup();
    }

    @Override
    public ThreadPoolReporter newThreadPool() {
        return new SemanticThreadPoolReporter(registry, id);
    }
}