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

package com.spotify.heroic.shell.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricType;
import lombok.Data;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class BackendKeyArgument {
    private final Series series;
    private final long base;
    private final MetricType type;
    private final Optional<Long> token;

    @JsonCreator
    public BackendKeyArgument(
        @JsonProperty("series") Series series, @JsonProperty("base") Long base,
        @JsonProperty("type") MetricType type, @JsonProperty("token") Optional<Long> token
    ) {
        this.series = checkNotNull(series, "series");
        this.base = checkNotNull(base, "base");
        this.type = Optional.ofNullable(type).orElse(MetricType.POINT);
        this.token = token;
    }

    public BackendKey toBackendKey() {
        return new BackendKey(series, base, type, token);
    }
}
