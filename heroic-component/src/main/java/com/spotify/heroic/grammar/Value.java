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

package com.spotify.heroic.grammar;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public interface Value {
    default Value sub(Value other) {
        throw context().error(String.format("%s: unsupported operator: -", this));
    }

    default Value add(Value other) {
        throw context().error(String.format("%s: unsupported operator: +", this));
    }

    <T> T cast(T to);

    <T> T cast(Class<T> to);

    Context context();

    default Optional<Value> toOptional() {
        return Optional.of(this);
    }

    static AggregationValue aggregation(String name) {
        return aggregation(name, list(), ImmutableMap.of());
    }

    static AggregationValue aggregation(String name, ListValue arguments) {
        return aggregation(name, arguments, ImmutableMap.of());
    }

    static AggregationValue aggregation(String name, ListValue arguments,
            Map<String, Value> keywords) {
        return new AggregationValue(name, arguments, keywords, Context.empty());
    }

    static ListValue list(Value... values) {
        return new ListValue(ImmutableList.copyOf(values), Context.empty());
    }

    static DurationValue duration(TimeUnit unit, long value) {
        return new DurationValue(unit, value, Context.empty());
    }

    static StringValue string(String string) {
        return new StringValue(string, Context.empty());
    }

    static IntValue number(long value) {
        return new IntValue(value, Context.empty());
    }

    static Value empty() {
        return new EmptyValue(Context.empty());
    }
}
