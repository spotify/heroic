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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.common.DateRange;

import eu.toolchain.async.Transform;
import lombok.Data;

@Data
public class QueryResultPart {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();

    /**
     * Groups of results.
     *
     * Failed groups are omitted from here, {@link #errors} for these.
     */
    private final List<ShardedResultGroup> groups;

    /**
     * Errors that happened during the query.
     */
    private final List<RequestError> errors;

    /**
     * Trace (replaces latency, eventually).
     */
    private final ShardTrace trace;

    public static Transform<ResultGroups, QueryResultPart> fromResultGroup(final DateRange range, final ClusterNode c) {
        final Stopwatch watch = Stopwatch.createStarted();

        return (ResultGroups result) -> new QueryResultPart(
                ImmutableList.copyOf(result.getGroups().stream().map(ResultGroup.fromResultGroup(c)).iterator()),
                result.getErrors(), ShardTrace.of(c.toString(), c.metadata(), watch.elapsed(TimeUnit.MILLISECONDS),
                        result.getStatistics(), Optional.empty()));
    }

    public boolean isEmpty() {
        return groups.stream().allMatch(ShardedResultGroup::isEmpty);
    }
}