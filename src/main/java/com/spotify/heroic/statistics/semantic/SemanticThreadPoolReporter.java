package com.spotify.heroic.statistics.semantic;

import lombok.RequiredArgsConstructor;

import com.codahale.metrics.Gauge;
import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.heroic.statistics.ThreadPoolReporterProvider;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

@RequiredArgsConstructor
public class SemanticThreadPoolReporter implements ThreadPoolReporter {
    private final SemanticMetricRegistry registry;
    private final MetricId id;

    @Override
    public Context newThreadPoolContext(String threadpool,
            final ThreadPoolReporterProvider provider) {
        final MetricId id = this.id.tagged("threadpool", threadpool);

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