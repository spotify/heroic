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
    private static final String COMPONENT = "metric-backend";

    private final SemanticMetricRegistry registry;
    private final MetricId id;
    private final CallbackReporter writeBatch;

    public SemanticMetricBackendReporter(SemanticMetricRegistry registry,
            String context) {
        this.registry = registry;

        this.id = MetricId.build().tagged("context", context, "component",
                COMPONENT);

        this.writeBatch = new SemanticCallbackReporter(registry, id.tagged(
                "what", "write-batch", "unit", Units.WRITES));
    }

    @Override
    public Context reportWriteBatch() {
        return writeBatch.setup();
    }

    @Override
    public void newWriteThreadPool(final ThreadPoolProvider provider) {
        registry.register(
                id.tagged("what", "write-thread-pool-size", "unit", Units.SIZE),
                new Gauge<Integer>() {
                    @Override
                    public Integer getValue() {
                        return provider.getQueueSize();
                    }
                });
    }
}
