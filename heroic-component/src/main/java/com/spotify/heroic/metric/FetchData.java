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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Ordering;
import com.spotify.heroic.common.Series;

import eu.toolchain.async.Collector;

@Data
public class FetchData {
    private final Series series;
    private final List<Long> times;
    private final List<MetricTypedGroup> groups;

    public static <T extends Metric> Collector<FetchData, FetchData> merger(final Series series) {
        return new Collector<FetchData, FetchData>() {
            @Override
            public FetchData collect(Collection<FetchData> results) throws Exception {
                final ImmutableList.Builder<Long> times = ImmutableList.builder();
                final Map<MetricType, ImmutableList.Builder<Metric>> fetchGroups = new HashMap<>();

                for (final FetchData fetch : results) {
                    times.addAll(fetch.times);

                    for (final MetricTypedGroup g : fetch.groups) {
                        ImmutableList.Builder<Metric> data = fetchGroups.get(g.getType());

                        if (data == null) {
                            data = new ImmutableList.Builder<>();
                            fetchGroups.put(g.getType(), data);
                        }

                        data.addAll(g.data);
                    }
                }

                final List<MetricTypedGroup> groups = fetchGroups
                        .entrySet()
                        .stream()
                        .map((e) -> new MetricTypedGroup(e.getKey(), Ordering.from(e.getKey().comparator())
                                .immutableSortedCopy(e.getValue().build()))).collect(Collectors.toList());

                return new FetchData(series, times.build(), groups);
            }
        };
    }
}