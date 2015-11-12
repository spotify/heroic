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

import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import lombok.Data;

@Data
public class Partition implements Aggregation {
    public static final String NAME = "partition";
    public static final Joiner params = Joiner.on(", ");

    private static final List<Aggregation> DEFAULT_CHILDREN = ImmutableList.of();

    private final List<Aggregation> children;

    @JsonCreator
    public Partition(@JsonProperty("children") List<Aggregation> children) {
        this.children = Optional.ofNullable(children).orElse(DEFAULT_CHILDREN);
    }

    @Override
    public Optional<Long> size() {
        return pickFrom(Math::max, Aggregation::size);
    }

    @Override
    public Optional<Long> extent() {
        return pickFrom(Math::max, Aggregation::extent);
    }

    private <T> Optional<T> pickFrom(final BiFunction<T, T, T> reducer,
            final Function<Aggregation, Optional<T>> value) {
        Optional<T> result = Optional.empty();

        for (final Aggregation a : children) {
            final Optional<T> candidate = value.apply(a);

            if (!result.isPresent()) {
                result = candidate;
                continue;
            }

            if (!candidate.isPresent()) {
                continue;
            }

            result = Optional.of(reducer.apply(result.get(), candidate.get()));
        }

        return result;
    }

    @Override
    public PartitionInstance apply(final AggregationContext context) {
        final List<AggregationInstance> children = ImmutableList
                .copyOf(this.children.stream().map((c) -> c.apply(context)).iterator());
        return new PartitionInstance(children);
    }

    @Override
    public String toDSL() {
        return String.format("%s(%s)", NAME,
                params.join(children.stream().map(Aggregation::toDSL).iterator()));
    }
}
