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

package com.spotify.heroic.aggregation;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.common.TimeUtils;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor(access = AccessLevel.PACKAGE)
public class SamplingQuery {
    private static final TimeUnit DEFAULT_UNIT = TimeUnit.MINUTES;

    private final Optional<Long> size;
    private final Optional<Long> extent;

    @JsonCreator
    public SamplingQuery(@JsonProperty("unit") String unitName, @JsonProperty("value") Long size,
            @JsonProperty("extent") Long extent) {
        final TimeUnit unit = TimeUtils.parseUnitName(unitName, DEFAULT_UNIT);
        this.size = Optional.fromNullable(size).transform((s) -> TimeUnit.MILLISECONDS.convert(s, unit));
        this.extent = Optional.fromNullable(extent).transform((s) -> TimeUnit.MILLISECONDS.convert(s, unit)).or(this.size);
    }

    public static SamplingQuery empty() {
        return new SamplingQuery(Optional.absent(), Optional.absent());
    }
}