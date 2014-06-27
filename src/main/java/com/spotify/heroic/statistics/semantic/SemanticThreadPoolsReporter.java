package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Gauge;
import com.spotify.heroic.statistics.ThreadPoolReporterProvider;
import com.spotify.heroic.statistics.ThreadPoolsReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticThreadPoolsReporter implements ThreadPoolsReporter {
    private final SemanticMetricRegistry registry;
    private final MetricId base;

    @Override
    public Context newThreadPoolContext(String string,
            final ThreadPoolReporterProvider provider) {
        final MetricId id = base.tagged("threadpool", string);
        final MetricId queueSize = id.tagged("what", "queue-size");

        registry.register(queueSize, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return provider.getQueueSize();
            }
        });

        return new Context() {
            @Override
            public void stop() {
                registry.remove(queueSize);
            }
        };
    }
}