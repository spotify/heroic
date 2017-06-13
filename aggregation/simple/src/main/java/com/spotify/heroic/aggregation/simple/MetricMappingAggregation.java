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

import static java.util.stream.Collectors.toList;

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
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.Data;

@Data
public abstract class MetricMappingAggregation implements AggregationInstance {
    private static final EmptyInstance INNER = EmptyInstance.INSTANCE;
    private final MetricMappingStrategy metricMappingStrategy;

    @Override
    public long estimate(DateRange range) {
        return INNER.estimate(range);
    }

    @Override
    public long cadence() {
        return 0;
    }

    @Override
    public AggregationSession session(
        DateRange range, RetainQuotaWatcher quotaWatcher, BucketStrategy bucketStrategy
    ) {
        return new Session(INNER.session(range, quotaWatcher, bucketStrategy));
    }

    @Override
    public AggregationInstance distributed() {
        return INNER;
    }

    class Session implements AggregationSession {

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
        ) {
            this.childSession.updateEvents(key, series, values);
        }

        @Override
        public void updatePayload(
            final Map<String, String> key, final Set<Series> series, final List<Payload> values
        ) {
            this.childSession.updatePayload(key, series, values);
        }

        @Override
        public void updateGroup(
            final Map<String, String> key, final Set<Series> series, final List<MetricGroup> values
        ) {
            this.childSession.updateGroup(key, series, values);
        }

        @Override
        public void updateSpreads(
            final Map<String, String> key, final Set<Series> series,
            final List<com.spotify.heroic.metric.Spread> values
        ) {
            this.childSession.updateSpreads(key, series, values);
        }

        @Override
        public AggregationResult result() {
            AggregationResult aggregationResult = this.childSession.result();
            List<AggregationOutput> outputs = aggregationResult
                .getResult()
                .stream()
                .map(aggregationOutput -> new AggregationOutput(
                    aggregationOutput.getKey(),
                    aggregationOutput.getSeries(),
                    metricMappingStrategy.apply(aggregationOutput.getMetrics())
                ))
                .collect(toList());
            return new AggregationResult(outputs, aggregationResult.getStatistics());
        }
    }
}
