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

import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Optionals;
import com.spotify.heroic.common.TimeUtils;
import java.beans.ConstructorProperties;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.Data;

@Data
public class SamplingQuery {
    private final Optional<Duration> size;
    private final Optional<Duration> extent;

    @ConstructorProperties({"unit", "value", "extent"})
    public SamplingQuery(String unit, Duration size, Duration extent) {
        final Optional<TimeUnit> u = TimeUtils.parseTimeUnit(unit);

        // XXX: prefer proper durations over unit override.
        if (u.isPresent()) {
            this.size = Optional.ofNullable(size).map(d -> d.withUnit(u.get()));
            this.extent = Optional.ofNullable(extent).map(d -> d.withUnit(u.get()));
        } else {
            this.size = Optional.ofNullable(size);
            this.extent = Optionals.firstPresent(Optional.ofNullable(extent), this.size);
        }
    }

    public SamplingQuery(final Optional<Duration> size, final Optional<Duration> extent) {
        this.size = size;
        this.extent = extent;
    }
}
