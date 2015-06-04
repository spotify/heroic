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

import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.utils.TimeUtils;

@Data
public class AggregationSamplingQuery {
    private static final TimeUnit DEFAULT_UNIT = TimeUnit.MINUTES;
    private static final long DEFAULT_VALUE = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

    public static final Supplier<AggregationSamplingQuery> DEFAULT_SUPPLIER = new Supplier<AggregationSamplingQuery>() {
        @Override
        public AggregationSamplingQuery get() {
            return new AggregationSamplingQuery(DEFAULT_VALUE, DEFAULT_VALUE);
        }
    };

    private final long size;
    private final long extent;

    @JsonCreator
    public static AggregationSamplingQuery create(@JsonProperty("unit") String unitName,
            @JsonProperty("value") Long inputSize, @JsonProperty("extent") Long inputExtent) {
        final TimeUnit unit = TimeUtils.parseUnitName(unitName, DEFAULT_UNIT);
        final long size = TimeUtils.parseSize(inputSize, unit, DEFAULT_VALUE);
        final long extent = TimeUtils.parseExtent(inputExtent, unit, size);
        return new AggregationSamplingQuery(size, extent);
    }

    public Sampling build() {
        return new Sampling(size, extent);
    }
}
