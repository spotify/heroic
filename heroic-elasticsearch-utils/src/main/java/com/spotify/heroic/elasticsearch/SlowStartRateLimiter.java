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

package com.spotify.heroic.elasticsearch;

import com.google.common.util.concurrent.RateLimiter;
import com.spotify.heroic.time.Clock;
import java.util.concurrent.TimeUnit;
import lombok.Data;

@Data
public class SlowStartRateLimiter implements SimpleRateLimiter {
    private final RateLimiter rateLimiter;
    private final double ratePerSecond;
    private final long slowStartDuration;
    private final TimeUnit slowStartTimeUnit;
    private final Clock clock;
    private final long startTime;

    /* Don't use an atomic type, to improve performance. The cost of this is that during startup
     * there might be some extra slow path calls verifying the time in threads until all threads
     * has seen the updated boolean value. No harm tho. This only affects startup performance and
     * improves performance after. Repeated calls to setRate() are ok. */
    private volatile boolean isSlowStartMode;

    public SlowStartRateLimiter(
        final double ratePerSecond, final double slowStartRatePerSecond,
        final long slowStartDuration, final TimeUnit slowStartTimeUnit
    ) {
        this(ratePerSecond, slowStartRatePerSecond, slowStartDuration, slowStartTimeUnit,
            Clock.system());
    }

    public SlowStartRateLimiter(
        final double ratePerSecond, final double slowStartRatePerSecond,
        final long slowStartDuration, final TimeUnit slowStartTimeUnit, final Clock clock
    ) {
        this.rateLimiter = RateLimiter.create(slowStartRatePerSecond);
        this.ratePerSecond = ratePerSecond;
        this.slowStartDuration = slowStartDuration;
        this.slowStartTimeUnit = slowStartTimeUnit;
        this.clock = clock;
        this.startTime = clock.currentTimeMillis();
        this.isSlowStartMode = true;
    }

    @Override
    public boolean tryAcquire() {
        return tryAcquire(1);
    }

    public boolean tryAcquire(final int num) {
        if (isSlowStartMode) {
            final long currTime = clock.currentTimeMillis();
            if (startTime + slowStartTimeUnit.toMillis(slowStartDuration) < currTime) {
                isSlowStartMode = false;
                rateLimiter.setRate(ratePerSecond);
            }
        }

        return rateLimiter.tryAcquire(num);
    }
}
