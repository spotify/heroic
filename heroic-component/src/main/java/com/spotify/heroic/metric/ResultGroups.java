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
import java.util.Collection;
import java.util.List;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.common.Statistics;

import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;

@Slf4j
@Data
public final class ResultGroups {
    private static final List<ResultGroup> EMPTY_GROUPS = new ArrayList<>();
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();

    public static final ResultGroups EMPTY = new ResultGroups(new ArrayList<ResultGroup>(), EMPTY_ERRORS,
            Statistics.EMPTY);

    private final List<ResultGroup> groups;
    private final List<RequestError> errors;
    private final Statistics statistics;

    @JsonCreator
    public ResultGroups(@JsonProperty("groups") List<ResultGroup> groups,
            @JsonProperty("errors") List<RequestError> errors, @JsonProperty("statistics") Statistics statistics) {
        this.groups = groups;
        this.errors = Optional.fromNullable(errors).or(EMPTY_ERRORS);
        this.statistics = statistics;
    }

    public static ResultGroups merge(Collection<ResultGroups> results) {
        final List<ResultGroup> groups = Lists.newArrayList();
        final List<RequestError> errors = Lists.newArrayList();
        Statistics statistics = Statistics.EMPTY;

        for (final ResultGroups r : results) {
            groups.addAll(r.groups);
            errors.addAll(r.errors);
            statistics = statistics.merge(r.statistics);
        }

        return new ResultGroups(groups, errors, statistics);
    }

    private static class SelfReducer implements Collector<ResultGroups, ResultGroups> {
        @Override
        public ResultGroups collect(Collection<ResultGroups> results) throws Exception {
            return merge(results);
        }
    }

    private static final SelfReducer merger = new SelfReducer();

    public static SelfReducer merger() {
        return merger;
    }

    public static final Transform<ResultGroups, ResultGroups> identity = new Transform<ResultGroups, ResultGroups>() {
        @Override
        public ResultGroups transform(ResultGroups result) throws Exception {
            return result;
        }
    };

    public static Transform<ResultGroups, ResultGroups> identity() {
        return identity;
    }

    public static ResultGroups seriesError(final List<TagValues> tags, Throwable e) {
        final List<RequestError> errors = Lists.newArrayList();
        errors.add(SeriesError.fromThrowable(tags, e));
        return new ResultGroups(EMPTY_GROUPS, errors, Statistics.EMPTY);
    }

    public static Transform<Throwable, ResultGroups> seriesError(final List<TagValues> tags) {
        return new Transform<Throwable, ResultGroups>() {
            @Override
            public ResultGroups transform(Throwable e) throws Exception {
                log.error("Encountered error in transform", e);
                return ResultGroups.seriesError(tags, e);
            }
        };
    }

    public static ResultGroups build(List<ResultGroup> groups, List<RequestError> errors, Statistics statistics) {
        return new ResultGroups(groups, errors, statistics);
    }

    public static Transform<Throwable, ResultGroups> nodeError(final ClusterNode.Group group) {
        return new Transform<Throwable, ResultGroups>() {
            @Override
            public ResultGroups transform(Throwable e) throws Exception {
                final List<RequestError> errors = ImmutableList.<RequestError> of(NodeError.fromThrowable(group.node(),
                        e));
                return new ResultGroups(EMPTY_GROUPS, errors, Statistics.EMPTY);
            }
        };
    }
}