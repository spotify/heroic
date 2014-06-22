package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.CallbackReporter.Context;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticBackendReporter implements MetricBackendReporter {
    private final CallbackReporter singleWrite;
    private final CallbackReporter batchWrite;

    public SemanticBackendReporter(SemanticMetricRegistry registry, String context) {
        final MetricId id = MetricId.build("backend").tagged("context", context);
        this.singleWrite = new SemanticCallbackReporter(registry,
                id.resolve("single-write"));
        this.batchWrite = new SemanticCallbackReporter(registry,
                id.resolve("batch-write"));
    }

    @Override
    public Context reportSingleWrite() {
        return singleWrite.setup();
    }

    @Override
    public Context reportWriteBatch() {
        return batchWrite.setup();
    }
}
