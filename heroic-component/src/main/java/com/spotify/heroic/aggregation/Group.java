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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.grammar.QueryParser;

import lombok.Data;

@Data
public class Group implements Aggregation {
    public static final String NAME = "group";
    public static final String ALL = "*";

    private final Optional<List<String>> of;
    private final Optional<Aggregation> each;

    @JsonCreator
    public Group(@JsonProperty("of") Optional<List<String>> of,
            @JsonProperty("each") Optional<Aggregation> each) {
        this.of = checkNotNull(of, "of");
        this.each = checkNotNull(each, "each");
    }

    @Override
    public Optional<Long> size() {
        return each.flatMap(Aggregation::size);
    }

    @Override
    public Optional<Long> extent() {
        return each.flatMap(Aggregation::extent);
    }

    @Override
    public GroupInstance apply(final AggregationContext context) {
        final AggregationInstance instance = each.orElse(Empty.INSTANCE).apply(context);

        final Optional<List<String>> of = this.of.map(o -> {
            final ImmutableSet.Builder<String> b = ImmutableSet.builder();
            b.addAll(o).addAll(context.requiredTags()).addAll(instance.requiredTags());
            return ImmutableList.copyOf(b.build());
        });

        return new GroupInstance(of, instance);
    }

    @Override
    public String toDSL() {
        final Aggregation each = this.each.orElse(Empty.INSTANCE);
        final String eachDSL = each instanceof Chain ? "(" + each.toDSL() + ")" : each.toDSL();
        final String ofDSL = of.map(QueryParser::escapeList).orElse(ALL);
        return eachDSL + " by " + ofDSL;
    }
}
