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

package com.spotify.heroic.generator.sine;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.generator.Generator;
import com.spotify.heroic.metric.Point;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;

public class SineGenerator implements Generator {
    /**
     * The magnitude (height) of the sine-wave.
     */
    private final double magnitude;

    /**
     * How many milliseconds should be a full period (2 * PI).
     */
    private final long period;

    /**
     * Frequency of data points in hertz.
     */
    private final long step;

    @Inject
    public SineGenerator(
        @Named("magnitude") double magnitude, @Named("period") Duration period,
        @Named("step") Duration step
    ) {
        this.magnitude = magnitude;
        this.period = period.toMilliseconds();
        this.step = step.toMilliseconds();
    }

    @Override
    public List<Point> generate(
        final Series series, final DateRange range
    ) {
        // calculate a consistent drift depending on which series is being fetched.
        double drift = Math.abs((double) series.hashCode() / (double) Integer.MAX_VALUE) * period;

        final ImmutableList.Builder<Point> data = ImmutableList.builder();

        final DateRange rounded = range.rounded(1000);

        for (long time = rounded.getStart(); time < rounded.getEnd(); time += step) {
            double offset = ((double) (time % period)) / (double) period;
            double value = Math.sin(Math.PI * 2 * (offset + drift)) * magnitude;
            data.add(new Point(time, value));
        }

        return data.build();
    }
}
