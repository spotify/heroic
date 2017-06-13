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
import com.google.common.base.Stopwatch;
import com.spotify.heroic.statistics.HeroicTimer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(of = {})
@RequiredArgsConstructor
public class SemanticHeroicTimerGauge implements HeroicTimer, Gauge<Long> {

    private final AtomicLong value = new AtomicLong(0);

    @Override
    public Context time() {
        final Stopwatch stopwatch = Stopwatch.createStarted();
        return new Context() {
            @Override
            public long stop() {
                final long elapsed = stopwatch.elapsed(TimeUnit.NANOSECONDS);
                value.set(elapsed);
                return elapsed;
            }

            @Override
            public void finished() throws Exception {
                stop();
            }
        };
    }

    @Override
    public Long getValue() {
        return value.get();
    }
}
