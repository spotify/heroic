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

import static com.spotify.heroic.common.Optionals.firstPresent;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.SamplingQuery;
import com.spotify.heroic.common.Duration;

import lombok.Data;

@Data
public abstract class SamplingAggregation implements Aggregation {
    public static final Joiner params = Joiner.on(", ");

    private final SamplingQuery sampling;

    @Override
    public AggregationInstance apply(final AggregationContext context) {
        final Duration s = firstPresent(sampling.getSize(), context.size()).orElseGet(context::defaultSize);
        final Duration e = firstPresent(sampling.getExtent(), context.extent()).orElseGet(context::defaultExtent);
        return apply(context, s.convert(TimeUnit.MILLISECONDS), e.convert(TimeUnit.MILLISECONDS));
    }

    @Override
    public Optional<Long> size() {
        return sampling.getSize().map(Duration::toMilliseconds);
    }

    @Override
    public Optional<Long> extent() {
        return sampling.getExtent().map(Duration::toMilliseconds);
    }

    protected String samplingDSL(final String name, final String... extra) {
        final List<String> arguments = new ArrayList<>();
        sampling.getSize().map(Duration::toDSL).ifPresent(arguments::add);
        sampling.getExtent().map(Duration::toDSL).ifPresent(arguments::add);

        for (int i = 0; i < extra.length / 2; i++) {
            arguments.add(extra[i * 2] + "=" + extra[i * 2 + 1]);
        }

        return String.format("%s(%s)", name, params.join(arguments));
    }

    protected abstract AggregationInstance apply(AggregationContext context, long size, long extent);
}