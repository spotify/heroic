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

import static com.spotify.heroic.common.Optionals.firstPresent;

import java.util.Optional;

import com.spotify.heroic.common.Duration;

import lombok.Data;

@Data
class OptionsContext implements AggregationContext {
    private final AggregationContext parent;
    private final Optional<Duration> size;
    private final Optional<Duration> extent;

    @Override
    public Optional<Duration> size() {
        return firstPresent(size, parent.size());
    }

    @Override
    public Optional<Duration> extent() {
        return firstPresent(extent, parent.extent());
    }

    @Override
    public Duration defaultSize() {
        return parent.defaultSize();
    }

    @Override
    public Duration defaultExtent() {
        return parent.defaultExtent();
    }
}
