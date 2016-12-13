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
import com.codahale.metrics.Meter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.HeroicTimer;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of = {})
public class SemanticFutureReporter implements FutureReporter {
    private final SemanticHeroicTimer timer;
    private final Meter failed;
    private final Meter resolved;
    private final Meter cancelled;
    private final Counter pending;

    public SemanticFutureReporter(SemanticMetricRegistry registry, MetricId id) {
        final String what = id.getTags().get("what");

        if (what == null) {
            throw new IllegalArgumentException("id does not provide the tag 'what'");
        }

        this.timer = new SemanticHeroicTimer(registry.timer(id.tagged("what", what + "-latency")));
        this.failed =
            registry.meter(id.tagged("what", what + "-failure-rate", "unit", Units.FAILURE));
        this.resolved =
            registry.meter(id.tagged("what", what + "-resolve-rate", "unit", Units.RESOLVE));
        this.cancelled =
            registry.meter(id.tagged("what", what + "-cancel-rate", "unit", Units.CANCEL));
        this.pending =
            registry.counter(id.tagged("what", what + "-pending", "unit", Units.COUNT));
    }

    @Override
    public FutureReporter.Context setup() {
        pending.inc();
        return new SemanticContext(timer.time());
    }

    @RequiredArgsConstructor
    private class SemanticContext implements FutureReporter.Context {
        private final HeroicTimer.Context context;

        @Override
        public void failed(Throwable e) throws Exception {
            pending.dec();
            failed.mark();
            context.stop();
        }

        @Override
        public void resolved(Object result) throws Exception {
            pending.dec();
            resolved.mark();
            context.stop();
        }

        @Override
        public void cancelled() throws Exception {
            pending.dec();
            cancelled.mark();
            context.stop();
        }
    }
}
