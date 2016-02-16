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
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationContext;
import lombok.Data;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class BelowK implements Aggregation {
    public static final String NAME = "belowk";

    private final double k;
    private final Aggregation of;

    @JsonCreator
    public BelowK(@JsonProperty("k") double k, @JsonProperty("of") Aggregation of) {
        this.k = k;
        this.of = checkNotNull(of, "of");
    }

    @Override
    public Optional<Long> size() {
        return of.size();
    }

    @Override
    public Optional<Long> extent() {
        return of.extent();
    }

    @Override
    public BelowKInstance apply(final AggregationContext context) {
        return new BelowKInstance(k, of.apply(context));
    }

    @Override
    public String toDSL() {
        return String.format("%s(%f, %s)", NAME, k, of.toDSL());
    }
}
