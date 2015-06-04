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
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.TimeData;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, of = { "NAME" })
public class CountUniqueAggregation extends BucketAggregation<TimeData, DataPoint, CountUniqueBucket> {
    public static final String NAME = "count-unique";

    public CountUniqueAggregation(Sampling sampling) {
        super(sampling, TimeData.class, DataPoint.class);
    }

    @JsonCreator
    public static CountUniqueAggregation create(@JsonProperty("sampling") Sampling sampling) {
        return new CountUniqueAggregation(sampling);
    }

    @Override
    protected CountUniqueBucket buildBucket(long timestamp) {
        return new CountUniqueBucket(timestamp);
    }

    @Override
    protected DataPoint build(CountUniqueBucket bucket) {
        final long count = bucket.count();

        if (count == 0)
            return new DataPoint(bucket.timestamp(), Double.NaN);

        return new DataPoint(bucket.timestamp(), count);
    }
}