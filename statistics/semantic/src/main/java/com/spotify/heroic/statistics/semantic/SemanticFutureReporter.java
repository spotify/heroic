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

import com.codahale.metrics.Counter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.HeroicTimer;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;

public class SemanticFutureReporter implements FutureReporter {
    private final SemanticHeroicTimer timer;
    private final Counter failed;
    private final Counter resolved;
    private final Counter cancelled;
    private final Counter pending;

    public SemanticFutureReporter(SemanticMetricRegistry registry, MetricId id) {
        final String what = id.getTags().get("what");

        if (what == null) {
            throw new IllegalArgumentException("id does not provide the tag 'what'");
        }

        this.timer = new SemanticHeroicTimer(registry.timer(id.tagged("what", what + "-latency")));
        this.failed =
            registry.counter(id.tagged("what", what + "-failed", "unit", Units.COUNT));
        this.resolved =
            registry.counter(id.tagged("what", what + "-resolved", "unit", Units.COUNT));
        this.cancelled =
            registry.counter(id.tagged("what", what + "-cancelled", "unit", Units.COUNT));
        this.pending =
            registry.counter(id.tagged("what", what + "-pending", "unit", Units.COUNT));
    }

    @Override
    public FutureReporter.Context setup() {
        pending.inc();
        return new SemanticContext(timer.time());
    }

    public String toString() {
        return "SemanticFutureReporter()";
    }

    private class SemanticContext implements FutureReporter.Context {
        private final HeroicTimer.Context context;

        @java.beans.ConstructorProperties({ "context" })
        public SemanticContext(final HeroicTimer.Context context) {
            this.context = context;
        }

        @Override
        public void failed(Throwable e) {
            pending.dec();
            failed.inc();
            context.stop();
        }

        @Override
        public void resolved(Object result) {
            pending.dec();
            resolved.inc();
            context.stop();
        }

        @Override
        public void cancelled() {
            pending.dec();
            cancelled.inc();
            context.stop();
        }
    }
}
