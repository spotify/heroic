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
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;

import com.spotify.heroic.metric.TdigestPoint;
import java.util.Map;

public abstract class AbstractBucket implements Bucket {
    @Override
    public void updatePoint(Map<String, String> key, Point sample) {
    }

    @Override
    public void updateDistributionPoint(Map<String, String> key, DistributionPoint sample) {
    }

    @Override
    public void updateSpread(Map<String, String> key, Spread sample) {
    }

    @Override
    public void updateGroup(Map<String, String> key, MetricGroup sample) {
    }

    @Override
    public void updatePayload(Map<String, String> key, Payload sample) {
    }

    @Override
    public void updateTDigestPoint(Map<String, String> key, TdigestPoint sample) {
    }
}
