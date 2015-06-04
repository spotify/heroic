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
import lombok.Getter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.Sampling;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, of = { "NAME", "q", "error" })
public class QuantileAggregation extends BucketAggregation<DataPoint, DataPoint, QuantileBucket> {
    public static final String NAME = "quantile";

    @Getter
    private final double q;

    @Getter
    private final double error;

    public QuantileAggregation(Sampling sampling, double q, double error) {
        super(sampling, DataPoint.class, DataPoint.class);
        this.q = q;
        this.error = error;
    }

    @JsonCreator
    public static QuantileAggregation create(@JsonProperty("sampling") Sampling sampling, @JsonProperty("q") Double q,
            @JsonProperty("error") Double error) {
        if (q == null)
            throw new RuntimeException("'q' is required");

        if (error == null)
            throw new RuntimeException("'error' is required");

        if (!(0 < error && error <= 1.0))
            throw new RuntimeException("'error' must be a value between 0 and 1 (inclusive).");

        return new QuantileAggregation(sampling, q, error);
    }

    @Override
    protected QuantileBucket buildBucket(long timestamp) {
        return new QuantileBucket(timestamp, q, error);
    }

    @Override
    protected DataPoint build(QuantileBucket bucket) {
        return new DataPoint(bucket.timestamp(), bucket.value());
    }
}