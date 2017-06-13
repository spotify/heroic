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
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.aggregation.AggregationCombiner;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import eu.toolchain.async.Collector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import lombok.Data;

@Data
public class QueryResult {
    /**
     * The range in which all result groups metric's should be contained in.
     */
    private final DateRange range;

    /**
     * Groups of results.
     * <p>
     * Failed groups are omitted from here, {@link #errors} for these.
     */
    private final List<ShardedResultGroup> groups;

    /**
     * Errors that happened during the query.
     */
    private final List<RequestError> errors;

    /**
     * Query trace, if available.
     */
    private final QueryTrace trace;

    private final ResultLimits limits;

    private final long preAggregationSampleSize;

    /**
     * Collect result parts into a complete result.
     *
     * @param range The range which the result represents.
     * @return A complete QueryResult.
     */
    public static Collector<QueryResultPart, QueryResult> collectParts(
        final QueryTrace.Identifier what, final DateRange range, final AggregationCombiner combiner,
        final OptionalLimit groupLimit
    ) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(what);

        return parts -> {
            final List<List<ShardedResultGroup>> all = new ArrayList<>();
            final List<RequestError> errors = new ArrayList<>();
            final ImmutableList.Builder<QueryTrace> queryTraces = ImmutableList.builder();
            final ImmutableSet.Builder<ResultLimit> limits = ImmutableSet.builder();
            long preAggregationSampleSize = 0;

            for (final QueryResultPart part : parts) {
                errors.addAll(part.getErrors());
                queryTraces.add(part.getQueryTrace());
                limits.addAll(part.getLimits().getLimits());
                preAggregationSampleSize += part.getPreAggregationSampleSize();

                if (part.isEmpty()) {
                    continue;
                }

                all.add(part.getGroups());
            }

            final List<ShardedResultGroup> groups = combiner.combine(all);
            final QueryTrace trace = w.end(queryTraces.build());

            if (groupLimit.isGreaterOrEqual(groups.size())) {
                limits.add(ResultLimit.GROUP);
            }

            return new QueryResult(range, groupLimit.limitList(groups), errors, trace,
                new ResultLimits(limits.build()), preAggregationSampleSize);
        };
    }

    public static QueryResult error(DateRange range, String errorMessage, QueryTrace trace) {
        return new QueryResult(range, Collections.emptyList(),
            Collections.singletonList(QueryError.fromMessage(errorMessage)), trace,
            ResultLimits.of(), 0);
    }
}
