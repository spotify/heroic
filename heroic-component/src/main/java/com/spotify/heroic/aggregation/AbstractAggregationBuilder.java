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

import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.grammar.AggregationValue;
import com.spotify.heroic.grammar.DiffValue;
import com.spotify.heroic.grammar.ListValue;
import com.spotify.heroic.grammar.Value;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractAggregationBuilder<T> implements AggregationBuilder<T> {
    protected final AggregationFactory factory;

    protected List<Aggregation> flatten(final AggregationContext context, final Value value) {
        if (value == null) {
            return ImmutableList.of();
        }

        if (value instanceof ListValue) {
            final ImmutableList.Builder<Aggregation> aggregations = ImmutableList.builder();

            for (final Value item : ((ListValue) value).getList()) {
                aggregations.addAll(flatten(context, item));
            }

            return aggregations.build();
        }

        final AggregationValue a = value.cast(AggregationValue.class);
        return ImmutableList.of(factory.build(context, a.getName(), a.getArguments(), a.getKeywordArguments()));
    }

    protected Optional<Long> parseDiffMillis(Deque<Value> args, Map<String, Value> keywords,
            String key) {
        if (!args.isEmpty()) {
            return Optional.of(args.removeFirst().cast(DiffValue.class).toMilliseconds());
        }

        if (keywords.containsKey(key)) {
            return Optional.of(keywords.get(key).cast(DiffValue.class).toMilliseconds());
        }

        return Optional.absent();
    }

    protected Aggregation parseAggregation(Map<String, Value> keywords, final Deque<Value> a,
            final OptionsContext c) {
        final AggregationValue aggregation;

        if (!a.isEmpty()) {
            aggregation = a.removeFirst().cast(AggregationValue.class);
        } else {
            if (!keywords.containsKey("aggregation")) {
                throw new IllegalArgumentException("Missing aggregation argument");
            }

            aggregation = keywords.get("aggregation").cast(AggregationValue.class);
        }

        return factory.build(c, aggregation.getName(), aggregation.getArguments(), aggregation.getKeywordArguments());
    }
}