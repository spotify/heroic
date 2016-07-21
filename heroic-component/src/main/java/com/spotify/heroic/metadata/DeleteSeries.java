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

package com.spotify.heroic.metadata;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.ArrayList;
import java.util.List;

@Data
public class DeleteSeries {
    private final List<RequestError> errors;
    private final int deleted;
    private final int failed;

    public static DeleteSeries of() {
        return new DeleteSeries(ImmutableList.of(), 0, 0);
    }

    public static DeleteSeries of(final int deleted, final int failed) {
        return new DeleteSeries(ImmutableList.of(), deleted, failed);
    }

    public static Collector<DeleteSeries, DeleteSeries> reduce() {
        return results -> {
            final List<RequestError> errors = new ArrayList<>();
            int deleted = 0;
            int failed = 0;

            for (final DeleteSeries result : results) {
                errors.addAll(result.errors);
                deleted += result.getDeleted();
                failed += result.getFailed();
            }

            return new DeleteSeries(errors, deleted, failed);
        };
    }

    public static Transform<Throwable, DeleteSeries> shardError(final ClusterShard shard) {
        return e -> new DeleteSeries(ImmutableList.of(ShardError.fromThrowable(shard, e)), 0, 0);
    }

    @Data
    public static class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
    }
}
