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

import lombok.EqualsAndHashCode;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.MetricType;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Spread;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, of = { "NAME" })
public class SpreadAggregation extends BucketAggregation<DataPoint, SpreadBucket> {
    public static final String NAME = "spread";

    public SpreadAggregation(Sampling sampling) {
        super(sampling, DataPoint.class, MetricType.SPREAD);
    }

    @JsonCreator
    public static SpreadAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new SpreadAggregation(sampling);
    }

    @Override
    protected SpreadBucket buildBucket(long timestamp) {
        return new SpreadBucket(timestamp);
    }

    @Override
    protected Spread build(SpreadBucket bucket) {
        return bucket.newSpread();
    }
}