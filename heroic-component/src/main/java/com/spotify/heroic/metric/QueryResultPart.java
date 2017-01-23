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
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.cluster.ClusterShard;
import eu.toolchain.async.Transform;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class QueryResultPart {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();

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
     * Query trace.
     */
    private final QueryTrace queryTrace;

    private final ResultLimits limits;

    private final long preAggregationSampleSize;

    public static Transform<FullQuery, QueryResultPart> fromResultGroup(
        final ClusterShard shard
    ) {
        return result -> {
            final ImmutableList<ShardedResultGroup> groups = ImmutableList.copyOf(result
                .getGroups()
                .stream()
                .map(ResultGroup.toShardedResultGroup(shard))
                .iterator());

            final long preAggregationSampleSize = result
                .getStatistics()
                .getCounterValue(AggregationInstance.SAMPLE_SIZE)
                .orElseGet(() -> {
                    long sum = 0;
                    for (final ShardedResultGroup g : groups) {
                        sum += g.getMetrics().getData().size();
                    }
                    return sum;
                });

            return new QueryResultPart(groups, result.getErrors(), result.getTrace(),
                result.getLimits(), preAggregationSampleSize);
        };
    }

    public boolean isEmpty() {
        return groups.stream().allMatch(ShardedResultGroup::isEmpty);
    }
}
