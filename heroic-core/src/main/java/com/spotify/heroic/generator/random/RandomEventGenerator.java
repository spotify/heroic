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

package com.spotify.heroic.generator.random;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.generator.Generator;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricCollection;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Map;
import java.util.Random;

public class RandomEventGenerator implements Generator {
    /**
     * Frequency of data points in hertz.
     */
    private final long step;

    private final Random random = new Random();

    @Inject
    public RandomEventGenerator(
        @Named("step") Duration step
    ) {
        this.step = step.toMilliseconds();
    }

    @Override
    public MetricCollection generate(
        final Series series, final DateRange range
    ) {
        final ImmutableList.Builder<Event> data = ImmutableList.builder();

        final DateRange rounded = range.rounded(1000);

        for (long time = rounded.getStart(); time < rounded.getEnd(); time += step) {
            if (random.nextDouble() > 0.9) {
                data.add(generateEvent(time));
            }
        }

        return MetricCollection.events(data.build());
    }

    private Event generateEvent(final long time) {
        return new Event(time, payload());
    }

    private Map<String, Object> payload() {
        final ImmutableMap.Builder<String, Object> payload = ImmutableMap.builder();
        payload.put("state", "ok");
        return payload.build();
    }
}
