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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import lombok.Data;

@Data
public class PartitionAggregationQuery implements AggregationQuery {
    private static final List<AggregationQuery> DEFAULT_CHILDREN = ImmutableList.of();

    private final List<AggregationQuery> children;

    @JsonCreator
    public PartitionAggregationQuery(@JsonProperty("children") List<AggregationQuery> children) {
        this.children = Optional.fromNullable(children).or(DEFAULT_CHILDREN);
    }

    @Override
    public PartitionAggregation build(final AggregationContext context) {
        final List<Aggregation> children = ImmutableList.copyOf(this.children.stream().map((c) -> c.build(context)).iterator());
        return new PartitionAggregation(children);
    }
}