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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import lombok.Data;

@Data
public class Chain implements Aggregation {
    public static final String NAME = "chain";
    public static final Joiner params = Joiner.on(", ");

    private final List<Aggregation> chain;

    @JsonCreator
    public Chain(@JsonProperty("chain") List<Aggregation> chain) {
        if (chain == null || chain.isEmpty()) {
            throw new IllegalArgumentException("chain must be specified and non-empty");
        }

        this.chain = chain;
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
        final ImmutableList<AggregationInstance> chain =
                ImmutableList.copyOf(this.chain.stream().map(c -> c.apply(context)).iterator());
        return new ChainInstance(chain);
    }

    @Override
    public String toDSL() {
        return String.format("%s(%s)", NAME,
                params.join(chain.stream().map(Aggregation::toDSL).iterator()));
    }
}
