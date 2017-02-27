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

import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.RuntimeNodeException;
import com.spotify.heroic.metric.Tracing;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.RetryException;
import eu.toolchain.async.RetryPolicy;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
public class ClusterShard {
    private static final QueryTrace.Identifier RETRY_BACKOFF =
        new QueryTrace.Identifier("retry-backoff");

    private final AsyncFramework async;

    private final Map<String, String> shard;
    private final List<ClusterNode.Group> groups;

    public <T> AsyncFuture<T> apply(
        Function<ClusterNode.Group, AsyncFuture<T>> function,
        BiFunction<T, List<QueryTrace>, T> handleRetryTraceFn, Tracing tracing
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
            .retryUntilResolved(() -> {
                final ClusterNode.Group next = it.next();
                return function.apply(next).catchFailed(throwable -> {
                    /* Actually never return;s, instead throws a new exception with added info.
                     * The point is to get Node identifying information into the exception */
                    throw new RuntimeNodeException(next.toString(), throwable.getMessage(),
                        throwable);
                });
            }, iteratorPolicy)
            .directTransform(retryResult -> handleRetryTraceFn.apply(retryResult.getResult(),
                queryTracesFromRetries(retryResult.getErrors(), retryResult.getBackoffTimings(),
                    tracing)));
    }

    private List<QueryTrace> queryTracesFromRetries(
        final List<RetryException> retryExceptions, final List<Long> backoffTimings,
        final Tracing tracing
    ) {
        final List<QueryTrace> traces = new ArrayList<>();
        long lastTS = 0;
        final Iterator<Long> backoffIterator = backoffTimings.iterator();
        for (final RetryException re : retryExceptions) {
            /* For each RetryException, add a QueryTrace in the current shard with information about
             * the cause and elapsed time */
            final long millisTS = re.getOffsetMillis();
            long elapsed = millisTS - lastTS;
            traces.add(tracing.newTrace(new QueryTrace.Identifier(getMessageFrom(re)), elapsed,
                TimeUnit.MILLISECONDS));
            lastTS = millisTS;

            /* After each retry, a backoff pause is inserted before trying the next node.
             * Here we add a QueryTrace to represent this, including the duration of the pause */
            if (!backoffIterator.hasNext()) {
                continue;
            }

            final long backoffTS = backoffIterator.next();
            elapsed = backoffTS - lastTS;
            traces.add(tracing.newTrace(RETRY_BACKOFF, elapsed, TimeUnit.MILLISECONDS));
            lastTS = backoffTS;
        }
        return traces;
    }

    private String getMessageFrom(final Throwable throwable) {
        final Throwable cause = throwable.getCause();
        if (cause instanceof RuntimeNodeException) {
            final RuntimeNodeException rne = (RuntimeNodeException) cause;
            return rne.getUri() + " error=" + cause.getMessage();
        }
        return "error=" + cause.getMessage();
    }
}
