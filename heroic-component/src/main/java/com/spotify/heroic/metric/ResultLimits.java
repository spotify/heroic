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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import lombok.Data;

import java.util.Set;

@Data
public class ResultLimits {
    private final Set<ResultLimit> limits;

    @JsonCreator
    public static ResultLimits create(final Set<ResultLimit> limits) {
        return new ResultLimits(limits);
    }

    @JsonValue
    public Set<ResultLimit> value() {
        return limits;
    }

    public ResultLimits join(final ResultLimits other) {
        return new ResultLimits(ImmutableSet.copyOf(Iterables.concat(limits, other.limits)));
    }

    public ResultLimits add(final ResultLimit limit) {
        return new ResultLimits(
            ImmutableSet.<ResultLimit>builder().add(limit).addAll(limits).build());
    }

    public static ResultLimits of(ResultLimit... limits) {
        return new ResultLimits(ImmutableSet.copyOf(limits));
    }
}
