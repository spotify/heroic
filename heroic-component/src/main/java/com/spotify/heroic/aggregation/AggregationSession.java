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

package com.spotify.heroic.aggregation;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

public interface AggregationSession {
    void updatePoints(Map<String, String> group, Set<Series> series, List<Point> values);

    void updateEvents(Map<String, String> group, Set<Series> series, List<Event> values);

    void updateSpreads(Map<String, String> group, Set<Series> series, List<Spread> values);

    void updateGroup(Map<String, String> group, Set<Series> series, List<MetricGroup> values);

    /**
     * Get the result of this aggregator.
     */
    AggregationResult result();
}
