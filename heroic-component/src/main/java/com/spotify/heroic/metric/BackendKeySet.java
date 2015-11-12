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

package com.spotify.heroic.metric;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;

import eu.toolchain.async.Collector;
import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class BackendKeySet implements Iterable<BackendKey> {
    private final List<BackendKey> keys;
    private final Optional<QueryTrace> trace;

    public BackendKeySet() {
        this(ImmutableList.of(), Optional.empty());
    }

    public BackendKeySet(List<BackendKey> keys) {
        this(keys, Optional.empty());
    }

    public int size() {
        return keys.size();
    }

    public boolean isEmpty() {
        return keys.isEmpty();
    }

    @Override
    public Iterator<BackendKey> iterator() {
        return keys.iterator();
    }

    public static Collector<BackendKeySet, BackendKeySet> collect(
            final QueryTrace.Identifier what) {
        final Stopwatch w = Stopwatch.createStarted();

        return results -> {
            final List<BackendKey> result = new ArrayList<>();
            final List<QueryTrace> children = new ArrayList<>();

            for (final BackendKeySet r : results) {
                result.addAll(r.getKeys());
                r.trace.ifPresent(children::add);
            }

            final Optional<QueryTrace> trace;

            if (!children.isEmpty()) {
                trace = Optional.of(new QueryTrace(what, w.elapsed(TimeUnit.NANOSECONDS),
                        ImmutableList.copyOf(children)));
            } else {
                trace = Optional.empty();
            }

            return new BackendKeySet(result, trace);
        };
    }
}
