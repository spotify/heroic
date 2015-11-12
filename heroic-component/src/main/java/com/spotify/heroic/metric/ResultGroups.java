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
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.QueryTrace.Identifier;

import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public final class ResultGroups {
    private static final List<ResultGroup> EMPTY_GROUPS = new ArrayList<>();
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();

    private final List<ResultGroup> groups;
    private final List<RequestError> errors;
    private final Statistics statistics;
    private final QueryTrace trace;

    @JsonCreator
    public ResultGroups(@JsonProperty("groups") List<ResultGroup> groups,
            @JsonProperty("errors") List<RequestError> errors,
            @JsonProperty("statistics") Statistics statistics,
            @JsonProperty("trace") QueryTrace trace) {
        this.groups = groups;
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.statistics = Objects.requireNonNull(statistics, "statistics");
        this.trace = Objects.requireNonNull(trace, "trace");
    }

    public static ResultGroups empty(final QueryTrace.Identifier what) {
        return new ResultGroups(ImmutableList.of(), ImmutableList.of(), Statistics.empty(),
                new QueryTrace(what));
    }

    public static Collector<ResultGroups, ResultGroups> collect(final QueryTrace.Identifier what) {
        final Stopwatch w = Stopwatch.createStarted();

        return results -> {
            final ImmutableList.Builder<ResultGroup> groups = ImmutableList.builder();
            final ImmutableList.Builder<RequestError> errors = ImmutableList.builder();
            final ImmutableList.Builder<QueryTrace> traces = ImmutableList.builder();

            Statistics statistics = Statistics.empty();

            for (final ResultGroups r : results) {
                groups.addAll(r.groups);
                errors.addAll(r.errors);
                traces.add(r.trace);
                statistics = statistics.merge(r.statistics);
            }

            return new ResultGroups(groups.build(), errors.build(), statistics,
                    new QueryTrace(what, w.elapsed(TimeUnit.NANOSECONDS), traces.build()));
        };
    }

    public static final Transform<ResultGroups, ResultGroups> identity =
            new Transform<ResultGroups, ResultGroups>() {
                @Override
                public ResultGroups transform(ResultGroups result) throws Exception {
                    return result;
                }
            };

    public static Transform<ResultGroups, ResultGroups> identity() {
        return identity;
    }

    public static ResultGroups seriesError(final QueryTrace.Identifier what,
            final List<TagValues> tags, final Throwable e) {
        final List<RequestError> errors = Lists.newArrayList();
        errors.add(SeriesError.fromThrowable(tags, e));
        return new ResultGroups(EMPTY_GROUPS, errors, Statistics.empty(), new QueryTrace(what));
    }

    public static Transform<Throwable, ResultGroups> seriesError(final QueryTrace.Identifier what,
            final List<TagValues> tags) {
        return new Transform<Throwable, ResultGroups>() {
            @Override
            public ResultGroups transform(Throwable e) throws Exception {
                log.error("Encountered error in transform", e);
                return ResultGroups.seriesError(what, tags, e);
            }
        };
    }

    public static Transform<Throwable, ResultGroups> nodeError(final QueryTrace.Identifier what,
            final ClusterNode.Group group) {
        return new Transform<Throwable, ResultGroups>() {
            @Override
            public ResultGroups transform(Throwable e) throws Exception {
                final List<RequestError> errors =
                        ImmutableList.<RequestError> of(NodeError.fromThrowable(group.node(), e));
                return new ResultGroups(EMPTY_GROUPS, errors, Statistics.empty(),
                        new QueryTrace(what));
            }
        };
    }

    public static Transform<ResultGroups, ResultGroups> trace(final Identifier what) {
        final Stopwatch w = Stopwatch.createStarted();

        return r -> new ResultGroups(r.groups, r.errors, r.statistics,
                new QueryTrace(what, w.elapsed(TimeUnit.NANOSECONDS), ImmutableList.of(r.trace)));
    }
}
