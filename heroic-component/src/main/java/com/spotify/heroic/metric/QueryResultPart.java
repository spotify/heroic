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
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Statistics;

import eu.toolchain.async.Transform;
import lombok.Data;

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

    public static Transform<ResultGroups, QueryResultPart> fromResultGroup(final DateRange range,
            final Map<String, String> shard) {
        final long start = System.currentTimeMillis();

        return (ResultGroups result) -> {
            final List<ShardedResultGroup> groups = new ArrayList<>();

            for (final ResultGroup group : result.getGroups()) {
                groups.add(new ShardedResultGroup(shard, group.getTags(), group.getGroup(), group.getCadence()));
            }

            final long end = System.currentTimeMillis();
            final long latency = end - start;
            final ImmutableList<ShardLatency> latencies = ImmutableList.of(new ShardLatency(shard, latency));

            return new QueryResultPart(groups, result.getStatistics(), result.getErrors(), latencies);
        };
    }
}