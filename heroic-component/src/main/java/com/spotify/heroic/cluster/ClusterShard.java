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

package com.spotify.heroic.cluster;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.RetryPolicy;
import eu.toolchain.async.RetryResult;
import lombok.Data;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Data
public class ClusterShard {
    private final AsyncFramework async;

    private final Map<String, String> shard;
    private final List<ClusterNode.Group> groups;

    public <T> AsyncFuture<T> apply(
        Function<ClusterNode.Group, AsyncFuture<T>> function
    ) {
        final Iterator<ClusterNode.Group> it = groups.iterator();

        if (!it.hasNext()) {
            return async.failed(new RuntimeException("No groups available"));
        }

        final RetryPolicy parent = RetryPolicy.timed(30000, RetryPolicy.exponential(100, 5000));

        /* a policy that is valid as long as there are more nodes available to try */
        final RetryPolicy iteratorPolicy = clockSource -> {
            final RetryPolicy.Instance p = parent.apply(clockSource);

            return () -> {
                if (it.hasNext()) {
                    return p.next();
                }

                return new RetryPolicy.Decision(false, 0);
            };
        };

        return async
            .retryUntilResolved(() -> function.apply(it.next()), iteratorPolicy)
            .directTransform(RetryResult::getResult);
    }
}
