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

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import eu.toolchain.async.Collector;

@Data
public class WriteResult {
    public static final WriteResult EMPTY = new WriteResult(ImmutableList.<Long> of());

    private final List<Long> times;

    @JsonCreator
    public WriteResult(@JsonProperty("times") List<Long> times) {
        this.times = times;
    }

    public static WriteResult of(Collection<Long> times) {
        return new WriteResult(ImmutableList.copyOf(times));
    }

    public static WriteResult of(long executionTime) {
        return new WriteResult(ImmutableList.of(executionTime));
    }

    public static WriteResult of() {
        return new WriteResult(ImmutableList.<Long> of());
    }

    public WriteResult merge(WriteResult other) {
        final List<Long> executionTime = new ArrayList<>(this.times);
        executionTime.addAll(other.times);
        return new WriteResult(executionTime);
    }

    private static class Merger implements Collector<WriteResult, WriteResult> {
        @Override
        public WriteResult collect(Collection<WriteResult> results) throws Exception {
            WriteResult result = EMPTY;

            for (final WriteResult r : results)
                result = result.merge(r);

            return result;
        }
    }

    private static final Merger merger = new Merger();

    public static Merger merger() {
        return merger;
    }
}