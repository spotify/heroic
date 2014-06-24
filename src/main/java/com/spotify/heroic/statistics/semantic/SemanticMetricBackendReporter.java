package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Gauge;
import com.spotify.heroic.statistics.CallbackReporter;
import com.spotify.heroic.statistics.CallbackReporter.Context;
import com.spotify.heroic.statistics.MetricBackendReporter;
import com.spotify.heroic.statistics.ThreadPoolProvider;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticMetricBackendReporter implements MetricBackendReporter {
    private final SemanticMetricRegistry registry;
    private final MetricId id;
    private final CallbackReporter writeBatch;

    public SemanticMetricBackendReporter(SemanticMetricRegistry registry,
            String context) {
        this.registry = registry;

        this.id = MetricId.build("metric-backend").tagged("context", context);

        this.writeBatch = new SemanticCallbackReporter(registry,
                id.resolve("write-batch"));
    }

    @Override
    public Context reportWriteBatch() {
        return writeBatch.setup();
    }

    @Override
    public void newWriteThreadPool(final ThreadPoolProvider provider) {
        final MetricId id = this.id.resolve("write-thread-pool");

        registry.register(id.tagged("what", "size", "unit", "count"),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return provider.getQueueSize();
                    }
                });
    }
}
