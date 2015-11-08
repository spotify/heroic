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

package com.spotify.heroic.common;

import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;

import lombok.Data;

/**
 * A helper type that represents a duration as the canonical duration/unit.
 *
 * This is provided so that we can implement a parser for it to simplify configurations that require durations.
 *
 * This type is intended to be conveniently de-serialize from a short-hand string type, like the following examples.
 *
 * <ul>
 * <li>1H - 1 Hour</li>
 * <li>5m - 5 minutes</li>
 * </ul>
 *
 * @author udoprog
 */
@Data
public class Duration {
    private final long duration;
    private final TimeUnit unit;

    public Duration(long duration, TimeUnit unit) {
        this.duration = duration;
        this.unit = Optional.fromNullable(unit).or(TimeUnit.SECONDS);
    }

    public long convert(final TimeUnit unit) {
        return unit.convert(this.duration, this.unit);
    }

    public static Duration of(long duration, TimeUnit unit) {
        return new Duration(duration, unit);
    }

    public Duration withUnit(final TimeUnit other) {
        return new Duration(duration, other);
    }
}