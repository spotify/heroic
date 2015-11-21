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
import java.util.ListIterator;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import lombok.Data;

@Data
public class Chain implements Aggregation {
    public static final String NAME = "chain";
    public static final Joiner PIPE = Joiner.on(" | ");

    private final List<Aggregation> chain;

    public Chain(List<Aggregation> chain) {
        this(Optional.ofNullable(chain));
    }

    @JsonCreator
    public Chain(@JsonProperty("chain") Optional<List<Aggregation>> chain) {
        this.chain = chain.filter(c -> !c.isEmpty()).orElseThrow(
                () -> new IllegalArgumentException("chain must be specified and non-empty"));
    }

    @Override
    public Optional<Long> size() {
        return chain.get(chain.size() - 1).size();
    }

    /**
     * The first aggregation in the chain determines the extent.
     */
    @Override
    public Optional<Long> extent() {
        return chain.iterator().next().extent();
    }

    @Override
    public ChainInstance apply(final AggregationContext context) {
        ListIterator<Aggregation> it = chain.listIterator(chain.size());

        AggregationContext current = context;
        final ImmutableSet.Builder<String> tags = ImmutableSet.builder();

        final ImmutableList.Builder<AggregationInstance> chain = ImmutableList.builder();

        while (it.hasPrevious()) {
            final AggregationInstance instance = it.previous().apply(current);
            tags.addAll(instance.requiredTags());
            current = AggregationContext.withRequiredTags(context, tags.build());
            chain.add(instance);
        }

        return new ChainInstance(Lists.reverse(chain.build()));
    }

    @Override
    public String toDSL() {
        return PIPE.join(chain.stream().map(Aggregation::toDSL).iterator());
    }

    @Override
    public String toString() {
        return toDSL();
    }
}
