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
import java.util.concurrent.TimeUnit;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.aggregation.AggregationCombiner;
import com.spotify.heroic.common.DateRange;

import eu.toolchain.async.Collector;
import lombok.Data;

@Data
public class QueryResult {
    /**
     * The range in which all result groups metric's should be contained in.
     */
    private final DateRange range;

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
     * Improve shard latencies.
     */
    private final List<ShardTrace> traces;

    /**
     * Query trace, if available.
     */
    private final QueryTrace trace;

    /**
     * Collect result parts into a complete result.
     *
     * @param range The range which the result represents.
     * @return A complete QueryResult.
     */
    public static Collector<QueryResultPart, QueryResult> collectParts(
            final QueryTrace.Identifier what, final DateRange range,
            final AggregationCombiner combiner) {
        final Stopwatch w = Stopwatch.createStarted();

        return parts -> {
            final List<List<ShardedResultGroup>> all = new ArrayList<>();
            final List<RequestError> errors = new ArrayList<>();
            final List<ShardTrace> traces = new ArrayList<>();
            final ImmutableList.Builder<QueryTrace> queryTraces = ImmutableList.builder();

            for (final QueryResultPart part : parts) {
                errors.addAll(part.getErrors());
                traces.add(part.getTrace());
                queryTraces.add(part.getQueryTrace());

                if (part.isEmpty()) {
                    continue;
                }

                all.add(part.getGroups());
            }

            final List<ShardedResultGroup> groups = combiner.combine(all);
            return new QueryResult(range, groups, errors, traces,
                    new QueryTrace(what, w.elapsed(TimeUnit.NANOSECONDS), queryTraces.build()));
        };
    }
}
