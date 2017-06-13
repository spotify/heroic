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

import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationOutput;
import com.spotify.heroic.aggregation.AggregationResult;
import com.spotify.heroic.aggregation.AggregationSession;
import com.spotify.heroic.aggregation.BucketStrategy;
import com.spotify.heroic.aggregation.EmptyInstance;
import com.spotify.heroic.aggregation.RetainQuotaWatcher;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Data;

@Data
public class DeltaInstance implements AggregationInstance {
    private static final EmptyInstance INNER = EmptyInstance.INSTANCE;

    @Override
    public long estimate(DateRange range) {
        return INNER.estimate(range);
    }

    @Override
    public long cadence() {
        return -1;
    }

    @Override
    public AggregationInstance distributed() {
        return this;
    }

    @Override
    public boolean distributable() {
        return false;
    }

    public List<Point> computeDiff(List<Point> points) {
        final Iterator<Point> it = points.iterator();
        final ArrayList<Point> result = new ArrayList<Point>();

        if (!it.hasNext()) {
            return Collections.emptyList();
        }

        Point previous = it.next();

        while (it.hasNext()) {
            final Point current = it.next();
            double diff = current.getValue() - previous.getValue();
            result.add(new Point(current.getTimestamp(), diff));
            previous = current;
        }

        return result;
    }

    @Override
    public AggregationSession session(
        DateRange range, RetainQuotaWatcher quotaWatcher, BucketStrategy bucketStrategy
    ) {
        return new Session(INNER.session(range, quotaWatcher, bucketStrategy));
    }

    private class Session implements AggregationSession {
        private final AggregationSession childSession;

        public Session(AggregationSession childSession) {
            this.childSession = childSession;
        }

        @Override
        public void updatePoints(
            final Map<String, String> key, final Set<Series> series, final List<Point> values
        ) {
            this.childSession.updatePoints(key, series, values);
        }

        @Override
        public void updateEvents(
            final Map<String, String> key, final Set<Series> series, final List<Event> values
        ) { }

        @Override
        public void updatePayload(
            final Map<String, String> key, final Set<Series> series, final List<Payload> values
        ) { }

        @Override
        public void updateGroup(
            final Map<String, String> key, final Set<Series> series, final List<MetricGroup> values
        ) { }

        @Override
        public void updateSpreads(
            final Map<String, String> key, final Set<Series> series, final List<Spread> values
        ) { }

        @Override
        public AggregationResult result() {
            AggregationResult aggregationResult = this.childSession.result();
            List<AggregationOutput> outputs = aggregationResult
                .getResult()
                .stream()
                .map(aggregationOutput -> new AggregationOutput(
                    aggregationOutput.getKey(),
                    aggregationOutput.getSeries(),
                    MetricCollection.build(
                        MetricType.POINT,
                        computeDiff(aggregationOutput.getMetrics().getDataAs(Point.class))
                    )
                ))
                .collect(Collectors.toList());
            return new AggregationResult(outputs, aggregationResult.getStatistics());
        }
    }
}
