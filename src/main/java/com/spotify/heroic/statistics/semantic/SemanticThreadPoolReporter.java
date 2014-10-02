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
    public Context newThreadPoolContext(String threadpool, final ThreadPoolReporterProvider provider) {
        final MetricId id = this.id.tagged("threadpool", threadpool);

        final MetricId queueSize = id.tagged("what", "queue-size");

        // amount of tasks on queue.
        registry.register(queueSize, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return provider.getQueueSize();
            }
        });

        // amount of tasks which can be queued.
        final MetricId queueCapacity = id.tagged("what", "queue-capacity");

        registry.register(queueCapacity, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return provider.getQueueCapacity();
            }
        });

        // amount of threads in pool.
        final MetricId poolSize = id.tagged("what", "pool-size");

        registry.register(poolSize, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return provider.getPoolSize();
            }
        });

        // amount of active threads in pool.
        final MetricId activeThreads = id.tagged("what", "active-threads");

        registry.register(activeThreads, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return provider.getActiveThreads();
            }
        });

        // amount of threads in core pool.
        final MetricId corePoolSize = id.tagged("what", "core-pool-size");

        registry.register(corePoolSize, new Gauge<Long>() {
            @Override
            public Long getValue() {
                return provider.getCorePoolSize();
            }
        });

        return new Context() {
            @Override
            public void stop() {
                registry.remove(queueSize);
                registry.remove(queueCapacity);
                registry.remove(activeThreads);
                registry.remove(corePoolSize);
                registry.remove(poolSize);
            }
        };
    }
}