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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import eu.toolchain.async.Collector;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Data
public class FetchData {
    private final QueryTrace trace;
    private final List<Long> times;
    private final List<MetricCollection> groups;
    private final boolean limited;

    public static FetchData limited(final QueryTrace trace) {
        return new FetchData(trace, ImmutableList.of(), ImmutableList.of(), true);
    }

    public static FetchData of(
        final QueryTrace trace, final List<Long> times, final List<MetricCollection> groups
    ) {
        return new FetchData(trace, times, groups, false);
    }

    public static Collector<FetchData, FetchData> collect(
        final QueryTrace.Identifier what
    ) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(what);

        return results -> {
            final ImmutableList.Builder<Long> times = ImmutableList.builder();
            final Map<MetricType, ImmutableList.Builder<Metric>> fetchGroups = new HashMap<>();
            final ImmutableList.Builder<QueryTrace> traces = ImmutableList.builder();
            boolean limited = false;

            for (final FetchData fetch : results) {
                times.addAll(fetch.times);
                traces.add(fetch.trace);

                for (final MetricCollection g : fetch.groups) {
                    ImmutableList.Builder<Metric> data = fetchGroups.get(g.getType());

                    if (data == null) {
                        data = new ImmutableList.Builder<>();
                        fetchGroups.put(g.getType(), data);
                    }

                    data.addAll(g.data);
                }

                limited |= fetch.limited;
            }

            final List<MetricCollection> groups = fetchGroups
                .entrySet()
                .stream()
                .map((e) -> MetricCollection.build(e.getKey(), Ordering
                    .from(e.getKey().comparator())
                    .immutableSortedCopy(e.getValue().build())))
                .collect(Collectors.toList());

            return new FetchData(w.end(traces.build()), times.build(), groups, limited);
        };
    }
}
