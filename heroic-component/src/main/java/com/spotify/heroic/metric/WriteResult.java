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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterNode;

import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

@Data
public class WriteResult {
    public static final List<RequestError> EMPTY_ERRORS = ImmutableList.of();
    private static final List<Long> EMPTY_TIMES = ImmutableList.<Long> of();

    public static final WriteResult EMPTY = new WriteResult(EMPTY_ERRORS, EMPTY_TIMES);

    private final List<RequestError> errors;
    private final List<Long> times;

    public WriteResult(List<Long> times) {
        this.errors = EMPTY_ERRORS;
        this.times = times;
    }

    @JsonCreator
    public WriteResult(@JsonProperty("errors") List<RequestError> errors,
            @JsonProperty("times") List<Long> times) {
        this.errors = errors;
        this.times = times;
    }

    public static WriteResult of(Collection<Long> times) {
        return new WriteResult(EMPTY_ERRORS, ImmutableList.copyOf(times));
    }

    public static WriteResult of(long duration) {
        return of(ImmutableList.of(duration));
    }

    public static WriteResult of() {
        return of(EMPTY_TIMES);
    }

    private static final Collector<WriteResult, WriteResult> collector = results -> {
        if (results.isEmpty()) {
            return WriteResult.EMPTY;
        }

        if (results.size() == 1) {
            return results.iterator().next();
        }

        final List<RequestError> errors = new ArrayList<>();
        final List<Long> times = new ArrayList<>();

        for (final WriteResult r : results) {
            errors.addAll(r.errors);
            times.addAll(r.times);
        }

        return new WriteResult(errors, times);
    };

    public static Collector<WriteResult, WriteResult> merger() {
        return collector;
    }

    public static Transform<Throwable, WriteResult> nodeError(final ClusterNode.Group group) {
        return new Transform<Throwable, WriteResult>() {
            @Override
            public WriteResult transform(Throwable e) throws Exception {
                final List<RequestError> errors =
                        ImmutableList.<RequestError> of(NodeError.fromThrowable(group.node(), e));
                return new WriteResult(errors, EMPTY_TIMES);
            }
        };
    }
}
