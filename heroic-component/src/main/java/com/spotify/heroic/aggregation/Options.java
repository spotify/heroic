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

import static com.spotify.heroic.common.Optionals.firstPresent;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.Duration;

import lombok.Data;

@Data
public class Options implements Aggregation {
    public static final String NAME = "opts";

    public static final long DEFAULT_SIZE = TimeUnit.MILLISECONDS.convert(60, TimeUnit.MINUTES);

    private final Optional<SamplingQuery> sampling;
    private final Optional<Aggregation> aggregation;

    @JsonCreator
    public Options(@JsonProperty("sampling") Optional<SamplingQuery> sampling,
            @JsonProperty("aggregation") Optional<Aggregation> aggregation) {
        this.sampling = sampling;
        this.aggregation = aggregation;
    }

    @Override
    public Optional<Long> size() {
        return firstPresent(aggregation.flatMap(Aggregation::size),
                sampling.flatMap(SamplingQuery::getSize).map(Duration::toMilliseconds));
    }

    @Override
    public Optional<Long> extent() {
        return firstPresent(aggregation.flatMap(Aggregation::extent),
                sampling.flatMap(SamplingQuery::getExtent).map(Duration::toMilliseconds));
    }

    @Override
    public AggregationInstance apply(final AggregationContext context) {
        return aggregation.orElse(Empty.INSTANCE)
                .apply(new OptionsContext(context, sampling.flatMap(SamplingQuery::getSize),
                        sampling.flatMap(SamplingQuery::getExtent)));
    }

    @Override
    public String toDSL() {
        return String.format("%s()", NAME);
    }
}
