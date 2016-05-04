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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.grammar.Value;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Utility class to contain a set of arguments and keywords in order to match the to aggregation
 * parameters and guarantee that all are consumed.
 */
public class AggregationArguments {
    private final LinkedList<Value> args;
    private final Map<String, Value> kw;

    public AggregationArguments(
        final List<? extends Value> args, final Map<String, ? extends Value> kw
    ) {
        this.args = new LinkedList<>(args);
        this.kw = new HashMap<>(kw);
    }

    /**
     * Take all arguments as a list with the given type.
     */
    public <T> List<T> takeArguments(final Class<T> expected) {
        final List<T> result =
            ImmutableList.copyOf(args.stream().map(v -> v.cast(expected)).iterator());
        args.clear();
        return result;
    }

    public <T> Optional<T> getNext(final String key, Class<T> expected) {
        if (!args.isEmpty()) {
            return Optional.of(args.removeFirst().cast(expected));
        }

        return Optional.ofNullable(kw.remove(key)).map(v -> v.cast(expected));
    }

    public <T> Optional<T> positional(Class<T> expected) {
        if (args.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(args.removeFirst().cast(expected));
    }

    public <T> Optional<T> keyword(final String key, Class<T> expected) {
        return Optional.ofNullable(kw.remove(key)).map(v -> v.cast(expected));
    }

    public void throwUnlessEmpty(final String name) {
        if (!args.isEmpty() || !kw.isEmpty()) {
            final List<String> parts = new ArrayList<>();
            final Joiner on = Joiner.on(" and ");

            if (!args.isEmpty()) {
                parts.add(
                    args.size() == 1 ? "argument " + args.iterator().next() : "arguments " + args);
            }

            if (!kw.isEmpty()) {
                parts.add((kw.size() == 1 ? "keyword " : "keywords ") + kw);
            }

            throw new IllegalStateException(name + ": has trailing " + on.join(parts));
        }
    }
}
