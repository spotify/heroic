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

import com.spotify.heroic.metric.DistributionPoint;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import com.spotify.heroic.metric.TdigestPoint;
import java.util.Map;

public interface AnyBucket extends Bucket {
    @Override
    default void updatePoint(Map<String, String> key, Point sample) {
        update(key, sample);
    }

    @Override
    default void updateSpread(Map<String, String> key, Spread sample) {
        update(key, sample);
    }

    @Override
    default void updateGroup(Map<String, String> key, MetricGroup sample) {
        update(key, sample);
    }

    @Override
    default void updatePayload(Map<String, String> key, Payload sample) {
        update(key, sample);
    }

    @Override
    default void updateDistributionPoint(Map<String, String> key, DistributionPoint sample) {
        update(key, sample); }

    @Override
    default void updateTDigestPoint(Map<String, String> key, TdigestPoint sample) {
        update(key, sample);
    }

    void update(Map<String, String> key, Metric sample);
}
