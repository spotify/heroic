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

package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.AbstractBucket;
import com.spotify.heroic.aggregation.Bucket;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;

public class GroupUniqueBucket extends AbstractBucket implements Bucket {
    final SortedSet<Point> points = new ConcurrentSkipListSet<>(Metric.comparator());
    final SortedSet<Event> events = new ConcurrentSkipListSet<>(Metric.comparator());
    final SortedSet<Spread> spreads = new ConcurrentSkipListSet<>(Metric.comparator());
    final SortedSet<MetricGroup> groups = new ConcurrentSkipListSet<>(Metric.comparator());

    final long timestamp;

    @java.beans.ConstructorProperties({ "timestamp" })
    public GroupUniqueBucket(final long timestamp) {
        this.timestamp = timestamp;
    }

    public List<MetricCollection> groups() {
        final ImmutableList.Builder<MetricCollection> result = ImmutableList.builder();

        if (!points.isEmpty()) {
            result.add(MetricCollection.points(ImmutableList.copyOf(points)));
        }

        if (!events.isEmpty()) {
            result.add(MetricCollection.events(ImmutableList.copyOf(events)));
        }

        if (!spreads.isEmpty()) {
            result.add(MetricCollection.spreads(ImmutableList.copyOf(spreads)));
        }

        if (!groups.isEmpty()) {
            result.add(MetricCollection.groups(ImmutableList.copyOf(groups)));
        }

        return result.build();
    }

    @Override
    public void updatePoint(Map<String, String> key, Point sample) {
        points.add(sample);
    }

    @Override
    public void updateEvent(Map<String, String> key, Event sample) {
        events.add(sample);
    }

    @Override
    public void updateSpread(Map<String, String> key, Spread sample) {
        spreads.add(sample);
    }

    @Override
    public void updateGroup(Map<String, String> key, MetricGroup sample) {
        groups.add(sample);
    }

    @Override
    public long timestamp() {
        return timestamp;
    }
}
