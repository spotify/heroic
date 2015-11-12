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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.grammar.ListValue;
import com.spotify.heroic.grammar.Value;

public abstract class GroupingAggregationBuilder extends AbstractAggregationDSL {
    public GroupingAggregationBuilder(AggregationFactory factory) {
        super(factory);
    }

    protected abstract Aggregation build(Optional<List<String>> over, Aggregation each);

    @Override
    public Aggregation build(final AggregationArguments args) {
        final Optional<List<String>> over =
                args.getNext("of", ListValue.class).map(this::convertOf);
        final Aggregation each =
                Aggregations.chain(args.getNext("each", Value.class).map(this::flatten));
        return build(over, each);
    }

    private List<String> convertOf(final ListValue list) {
        return ImmutableList
                .copyOf(list.getList().stream().map(v -> v.cast(String.class)).iterator());
    }
}
