/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.statistics.semantic;

import com.codahale.metrics.Gauge;
import com.spotify.heroic.statistics.ThreadPoolReporter;
import com.spotify.heroic.statistics.ThreadPoolReporterProvider;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of = {"base"})
@RequiredArgsConstructor
public class SemanticThreadPoolReporter implements ThreadPoolReporter {
    private final SemanticMetricRegistry registry;
    private final MetricId base;

    @Override
    public Context newThreadPoolContext(
        String threadpool, final ThreadPoolReporterProvider provider
    ) {
        final MetricId id = this.base.tagged("threadpool", threadpool);

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
