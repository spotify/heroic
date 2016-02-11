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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import lombok.Data;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A helper type that represents a duration as the canonical duration/unit.
 * <p>
 * This is provided so that we can implement a parser for it to simplify configurations that require
 * durations.
 * <p>
 * This type is intended to be conveniently de-serialize from a short-hand string type, like the
 * following examples.
 * <p>
 * <ul> <li>1H - 1 Hour</li> <li>5m - 5 minutes</li> </ul>
 *
 * @author udoprog
 */
@Data
public class Duration {
    public static final TimeUnit DEFAULT_UNIT = TimeUnit.MILLISECONDS;

    private final long duration;
    private final TimeUnit unit;

    public Duration(long duration, TimeUnit unit) {
        this.duration = duration;
        this.unit = Optional.fromNullable(unit).or(TimeUnit.SECONDS);
    }

    public long convert(final TimeUnit unit) {
        return unit.convert(this.duration, this.unit);
    }

    public long toMilliseconds() {
        return convert(TimeUnit.MILLISECONDS);
    }

    public static Duration of(long duration, TimeUnit unit) {
        return new Duration(duration, unit);
    }

    public static Duration ofMilliseconds(long duration) {
        return of(duration, TimeUnit.MILLISECONDS);
    }

    public Duration withUnit(final TimeUnit other) {
        return new Duration(duration, other);
    }

    public String toDSL() {
        return Long.toString(duration) + unitSuffix(unit);
    }

    private static final Pattern PATTERN = Pattern.compile("^(\\d+)([a-zA-Z]*)$");

    // @formatter:off
    private static Map<String, TimeUnit> units = ImmutableMap.<String, TimeUnit>builder()
        .put("ms", TimeUnit.MILLISECONDS)
        .put("s", TimeUnit.SECONDS)
        .put("m", TimeUnit.MINUTES)
        .put("h", TimeUnit.HOURS)
        .put("H", TimeUnit.HOURS)
        .put("d", TimeUnit.DAYS)
    .build();
    // @formatter:on

    public static Duration parseDuration(final String string) {
        final Matcher m = PATTERN.matcher(string);

        if (!m.matches()) {
            throw new IllegalArgumentException("Invalid duration: " + string);
        }

        final long duration = Long.parseLong(m.group(1));
        final String unitString = m.group(2);

        if (unitString.isEmpty()) {
            return new Duration(duration, DEFAULT_UNIT);
        }

        if ("w".equals(unitString)) {
            return new Duration(duration * 7, TimeUnit.DAYS);
        }

        final TimeUnit unit = units.get(unitString);

        if (unit == null) {
            throw new IllegalArgumentException(
                "Invalid unit (" + unitString + ") in duration: " + string);
        }

        return new Duration(duration, unit);
    }

    public static String unitSuffix(TimeUnit unit) {
        switch (unit) {
            case MILLISECONDS:
                return "ms";
            case SECONDS:
                return "s";
            case MINUTES:
                return "m";
            case HOURS:
                return "h";
            case DAYS:
                return "d";
            default:
                throw new IllegalStateException("Unit not supported for serialization: " + unit);
        }
    }
}
