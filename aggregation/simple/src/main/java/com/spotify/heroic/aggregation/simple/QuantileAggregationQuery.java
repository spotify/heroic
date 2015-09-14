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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.SamplingQuery;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper = true)
public class QuantileAggregationQuery extends SamplingAggregationQuery {
    public static final double DEFAULT_QUANTILE = 0.5;
    public static final double DEFAULT_ERROR = 0.01;

    private final double q;
    private final double error;

    @JsonCreator
    public QuantileAggregationQuery(@JsonProperty("sampling") SamplingQuery sampling, @JsonProperty("q") Double q,
            @JsonProperty("error") Double error) {
        super(Optional.fromNullable(sampling).or(SamplingQuery::empty));
        this.q = Optional.fromNullable(q).or(DEFAULT_QUANTILE);
        this.error = Optional.fromNullable(error).or(DEFAULT_ERROR);
    }

    @Override
    public QuantileAggregation build(AggregationContext context, final long size, final long extent) {
        return new QuantileAggregation(size, extent, q, error);
    }
}