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

import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;

import eu.toolchain.async.Collector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Data;

@Data
public class FetchData {
    private final Result result;
    private final List<Long> times;
    private final List<MetricCollection> groups;

    public static Result errorResult(final QueryTrace trace, final RequestError error) {
        return new Result(trace, ImmutableList.of(error));
    }

    public static Result result(final QueryTrace trace) {
        return new Result(trace, ImmutableList.of());
    }

    @Deprecated
    public static FetchData error(final QueryTrace trace, final RequestError error) {
        return new FetchData(errorResult(trace, error), ImmutableList.of(), ImmutableList.of());
    }

    @Deprecated
    public static FetchData of(
        final QueryTrace trace, final List<Long> times, final List<MetricCollection> groups
    ) {
        return new FetchData(new Result(trace, ImmutableList.of()), times, groups);
    }

    public static Collector<Result, Result> collectResult(
        final QueryTrace.Identifier what
    ) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(what);

        return results -> {
            final ImmutableList.Builder<QueryTrace> traces = ImmutableList.builder();
            final ImmutableList.Builder<RequestError> errors = ImmutableList.builder();

            for (final Result result : results) {
                traces.add(result.trace);
                errors.addAll(result.errors);
            }
            return new Result(w.end(traces.build()), errors.build());
        };
    }

    @Deprecated
    public static Collector<FetchData, FetchData> collect(
        final QueryTrace.Identifier what
    ) {
        final Collector<Result, Result> resultCollector = collectResult(what);

        return fetchDataCollection -> {
            final ImmutableList.Builder<Long> times = ImmutableList.builder();
            final Map<MetricType, ImmutableList.Builder<Metric>> fetchGroups = new HashMap<>();
            final ImmutableList.Builder<Result> results = ImmutableList.builder();

            for (final FetchData fetch : fetchDataCollection) {
                times.addAll(fetch.times);
                results.add(fetch.result);

                for (final MetricCollection g : fetch.groups) {
                    ImmutableList.Builder<Metric> data = fetchGroups.get(g.getType());

                    if (data == null) {
                        data = new ImmutableList.Builder<>();
                        fetchGroups.put(g.getType(), data);
                    }

                    data.addAll(g.data);
                }
            }

            final List<MetricCollection> groups = fetchGroups
                .entrySet()
                .stream()
                .map((e) -> MetricCollection.build(e.getKey(),
                    Ordering.from(Metric.comparator()).immutableSortedCopy(e.getValue().build())))
                .collect(Collectors.toList());

            return new FetchData(resultCollector.collect(results.build()), times.build(), groups);
        };
    }

    @Data
    public static class Request {
        private final MetricType type;
        private final Series series;
        private final DateRange range;
        private final QueryOptions options;
    }

    @Data
    public static class Result {
        private final QueryTrace trace;
        private final List<RequestError> errors;
    }
}
