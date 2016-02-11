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
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.SamplingQuery;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Optionals;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Data
@EqualsAndHashCode(callSuper = true)
public class Quantile extends SamplingAggregation {
    public static final String NAME = "quantile";

    public static final double DEFAULT_QUANTILE = 0.5;
    public static final double DEFAULT_ERROR = 0.01;

    private final Optional<Double> q;
    private final Optional<Double> error;

    @JsonCreator
    public Quantile(
        @JsonProperty("sampling") Optional<SamplingQuery> sampling,
        @JsonProperty("size") Optional<Duration> size,
        @JsonProperty("extent") Optional<Duration> extent, @JsonProperty("q") Optional<Double> q,
        @JsonProperty("error") Optional<Double> error
    ) {
        super(Optionals.firstPresent(size, sampling.flatMap(SamplingQuery::getSize)),
            Optionals.firstPresent(extent, sampling.flatMap(SamplingQuery::getExtent)));
        this.q = q;
        this.error = error;
    }

    @Override
    public QuantileInstance apply(AggregationContext context, final long size, final long extent) {
        return new QuantileInstance(size, extent, q.orElse(DEFAULT_QUANTILE),
            error.orElse(DEFAULT_ERROR));
    }

    @Override
    public String toDSL() {
        final List<String> extra = new ArrayList<>();

        this.q.ifPresent(q -> extra.add("q=" + percentage(q)));
        this.error.ifPresent(error -> extra.add("error=" + percentage(error)));

        return samplingDSL(NAME, extra);
    }

    private String percentage(double v) {
        return Integer.toString((int) Math.min(100, Math.max(0, Math.round(v * 100))));
    }
}
