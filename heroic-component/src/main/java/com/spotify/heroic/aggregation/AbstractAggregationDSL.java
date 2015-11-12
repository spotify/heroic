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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.grammar.AggregationValue;
import com.spotify.heroic.grammar.ListValue;
import com.spotify.heroic.grammar.Value;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractAggregationDSL implements AggregationDSL {
    private final AggregationFactory factory;

    protected List<Aggregation> flatten(final AggregationArguments args) {
        final ImmutableList.Builder<Aggregation> aggregations = ImmutableList.builder();
        args.takeArguments(Value.class).stream().map(this::flatten).forEach(aggregations::addAll);
        return aggregations.build();
    }

    protected List<Aggregation> flatten(final Value value) {
        if (value instanceof ListValue) {
            final ImmutableList.Builder<Aggregation> aggregations = ImmutableList.builder();

            for (final Value item : ((ListValue) value).getList()) {
                aggregations.addAll(flatten(item));
            }

            return aggregations.build();
        }

        return ImmutableList.of(asAggregation(value.cast(AggregationValue.class)));
    }

    protected Aggregation asAggregation(final AggregationValue value) {
        return value.build(factory);
    }
}
