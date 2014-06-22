package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.CallbackReporter.Context;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticMetricBackendReporter implements MetricBackendReporter {
    private final CallbackReporter writeBatch;

    public SemanticMetricBackendReporter(SemanticMetricRegistry registry, String context) {
        final MetricId id = MetricId.build("metric-backend").tagged("context",
                context);
        this.writeBatch = new SemanticCallbackReporter(registry,
                id.resolve("write-batch"));
    }

    @Override
    public Context reportWriteBatch() {
        return writeBatch.setup();
    }
}
