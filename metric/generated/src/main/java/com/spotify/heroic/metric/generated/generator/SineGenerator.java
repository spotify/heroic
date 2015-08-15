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

package com.spotify.heroic.metric.generated.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.generated.Generator;

public class SineGenerator implements Generator {
    private static final Map<String, Object> PAYLOAD = ImmutableMap.<String, Object> of();

    @Inject
    @Named("magnitude")
    private double magnitude;

    /**
     * How many milliseconds should be a full period (2 * PI).
     */
    @Inject
    @Named("period")
    private long period;

    /**
     * Frequency of data points in hertz.
     */
    @Inject
    @Named("step")
    private long step;

    @Override
    public List<Metric> generate(Series series, DateRange range, FetchQuotaWatcher watcher) {
        // calculate a consistent drift depending on which series is being fetched.
        double drift = Math.abs((double) series.hashCode() / (double) Integer.MAX_VALUE);

        final List<Metric> data = new ArrayList<>();

        final DateRange rounded = range.rounded(1000);

        if (!watcher.readData(range.diff() / step))
            throw new IllegalArgumentException("data limit reached");

        for (long time = rounded.getStart(); time < rounded.getEnd(); time += step) {
            double offset = ((double) (time % period)) / (double) period;
            double value = Math.sin(Math.PI * 2 * (offset + drift)) * magnitude;
            data.add(new Point(time, value));
        }

        return data;
    }

    @Override
    public List<Metric> generateEvents(Series series, DateRange range, FetchQuotaWatcher watcher) {
        final List<Metric> data = new ArrayList<>();

        final DateRange rounded = range.rounded(1000);

        if (!watcher.readData(range.diff() / step))
            throw new IllegalArgumentException("data limit reached");

        for (long time = rounded.getStart(); time < rounded.getEnd(); time += step) {
            data.add(new Event(time, PAYLOAD));
        }

        return data;
    }
}
