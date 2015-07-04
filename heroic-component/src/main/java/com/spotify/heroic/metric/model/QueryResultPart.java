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

package com.spotify.heroic.metric.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Statistics;

import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;

@Slf4j
@Data
public class QueryResultPart {
    private static final List<ShardedResultGroup> EMPTY_GROUPS = new ArrayList<>();
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final List<ShardLatency> EMPTY_LATENCIES = ImmutableList.of();

    /**
     * Groups of results.
     *
     * Failed groups are omitted from here, {@link #errors} for these.
     */
    private final List<ShardedResultGroup> groups;

    /**
     * Statistics about the query.
     */
    private final Statistics statistics;

    /**
     * Errors that happened during the query.
     */
    private final List<RequestError> errors;

    /**
     * Shard latencies associated with the query.
     */
    private final List<ShardLatency> latencies;

    private static final Collector<QueryResultPart, QueryResultPart> collector = new Collector<QueryResultPart, QueryResultPart>() {
        @Override
        public QueryResultPart collect(Collection<QueryResultPart> results) throws Exception {
            Statistics statistics = Statistics.EMPTY;

            final List<ShardedResultGroup> groups = new ArrayList<>();
            final List<RequestError> errors = new ArrayList<>();
            final List<ShardLatency> latencies = new ArrayList<>();

            for (final QueryResultPart result : results) {
                statistics = statistics.merge(result.getStatistics());
                groups.addAll(result.getGroups());
                errors.addAll(result.getErrors());
                latencies.addAll(result.getLatencies());
            }

            return new QueryResultPart(groups, statistics, errors, latencies);
        }
    };

    public static Collector<QueryResultPart, QueryResultPart> reduce() {
        return collector;
    }

    public static Transform<ResultGroups, QueryResultPart> toSharded(final DateRange range, final Map<String, String> shard) {
        final long start = System.currentTimeMillis();

        return new Transform<ResultGroups, QueryResultPart>() {
            @Override
            public QueryResultPart transform(ResultGroups result) throws Exception {
                final List<ShardedResultGroup> groups = new ArrayList<>();

                for (final ResultGroup group : result.getGroups()) {
                    groups.add(new ShardedResultGroup(shard, group.getTags(), group.getValues(), group.getType()));
                }

                final long end = System.currentTimeMillis();
                final long latency = end - start;
                final ImmutableList<ShardLatency> latencies = ImmutableList.of(new ShardLatency(shard, latency));

                return new QueryResultPart(groups, result.getStatistics(), result.getErrors(), latencies);
            }
        };
    }

    public static QueryResultPart nodeError(final UUID id, final String node, final Map<String, String> tags,
            Throwable e) {
        final List<RequestError> errors = Lists.newArrayList();
        errors.add(NodeError.fromThrowable(id, node, tags, e));
        return new QueryResultPart(EMPTY_GROUPS, Statistics.EMPTY, errors, EMPTY_LATENCIES);
    }

    public static Transform<Throwable, QueryResultPart> nodeError(final UUID id, final String node,
            final Map<String, String> shard) {
        final long start = System.currentTimeMillis();

        return new Transform<Throwable, QueryResultPart>() {
            @Override
            public QueryResultPart transform(Throwable e) throws Exception {
                log.error("Encountered error in transform", e);
                final long end = System.currentTimeMillis();
                final long latency = end - start;
                final ImmutableList<ShardLatency> latencies = ImmutableList.of(new ShardLatency(shard, latency));

                final List<RequestError> errors = Lists.newArrayList();
                errors.add(NodeError.fromThrowable(id, node, shard, e));
                return new QueryResultPart(EMPTY_GROUPS, Statistics.EMPTY, errors, latencies);
            }
        };
    }
}