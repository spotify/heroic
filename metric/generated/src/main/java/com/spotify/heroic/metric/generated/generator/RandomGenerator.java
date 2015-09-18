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
import java.util.Random;

import javax.inject.Inject;
import javax.inject.Named;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.generated.Generator;

/**
 * A generator that generates pseudo random numbers depending on which serie and time range is required.
 *
 * The same serie and time range should always return the same values, making this usable across restarts for
 * troubleshooting.
 *
 * @author udoprog
 */
public class RandomGenerator implements Generator {
    private static final Map<String, Object> PAYLOAD = ImmutableMap.of();

    @Inject
    @Named("min")
    private double min;

    @Inject
    @Named("max")
    private double max;

    @Inject
    @Named("range")
    private double range;

    @Inject
    @Named("step")
    private long step;

    @Override
    public List<Point> generatePoints(Series series, DateRange range, FetchQuotaWatcher watcher) {
        final double diff = max - min;

        int seriesHash = series.hashCode();

        final double localMin = min + diff * seriesRand(seriesHash);

        final List<Point> data = new ArrayList<>();

        final long start = calculateStart(range.getStart());

        if (!watcher.readData(range.diff() / step))
            throw new IllegalArgumentException("data limit reached");

        for (long i = start; i < range.getEnd(); i += step) {
            final Double value = localMin + (positionRand(seriesHash, i) - 0.5) * this.range;
            data.add(new Point(i, value));
        }

        return data;
    }

    @Override
    public List<Event> generateEvents(Series series, DateRange range, FetchQuotaWatcher watcher) {
        final List<Event> data = new ArrayList<>();

        final DateRange rounded = range.rounded(1000);

        if (!watcher.readData(rounded.diff() / step))
            throw new IllegalArgumentException("data limit reached");

        for (long time = rounded.getStart(); time < rounded.getEnd(); time += step) {
            data.add(new Event(time, PAYLOAD));
        }

        return data;
    }

    private long calculateStart(long start) {
        return start + (start % step == 0 ? 0 : (step - (start % step)));
    }

    private double seriesRand(int seriesHash) {
        return new Random(seriesHash).nextDouble();
    }

    private double positionRand(int seriesHash, long position) {
        final int prime = 31;
        int result = 1;
        result = prime * result + seriesHash;
        result = prime * result + ((Long) position).hashCode();
        return new Random(result).nextDouble();
    }
}