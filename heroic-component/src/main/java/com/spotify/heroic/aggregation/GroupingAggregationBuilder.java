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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.grammar.AggregationValue;
import com.spotify.heroic.grammar.ListValue;
import com.spotify.heroic.grammar.Value;

import java.util.List;
import java.util.Optional;

public abstract class GroupingAggregationBuilder extends AbstractAggregationDSL {
    public GroupingAggregationBuilder(AggregationFactory factory) {
        super(factory);
    }

    protected abstract Aggregation build(Optional<List<String>> of, Optional<Aggregation> each);

    @Override
    public Aggregation build(final AggregationArguments args) {
        final Optional<List<String>> of =
            args.getNext("of", Value.class).flatMap(Value::toOptional).map(this::convertOf);
        final Optional<Aggregation> each =
            args.getNext("each", AggregationValue.class).map(this::asAggregation);
        return build(of, each);
    }

    private List<String> convertOf(final Value list) {
        return ImmutableList.copyOf(list
            .cast(ListValue.class)
            .getList()
            .stream()
            .map(v -> v.cast(String.class))
            .iterator());
    }
}
