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

package com.spotify.heroic.grammar;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import lombok.AccessLevel;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.beans.ConstructorProperties;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Optional;

@Data
@EqualsAndHashCode(exclude = {"ctx"})
@JsonTypeName("time")
@RequiredArgsConstructor
public class TimeExpression implements Expression {
    // @formatter:off
    public static final List<DateTimeFormatter> FORMATTERS = ImmutableList.of(
        DateTimeFormatter.ofPattern("HH:mm"),
        DateTimeFormatter.ofPattern("HH:mm:ss"),
        DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
    );
    // @formatter:On

    @Getter(AccessLevel.NONE)
    private final Context ctx;

    private final int hours;
    private final int minutes;
    private final int seconds;
    private final int milliSeconds;

    @ConstructorProperties({"hours", "minutes", "seconds", "milliSeconds"})
    public TimeExpression(
        final int hours, final int minutes, final int seconds, final int milliSeconds
    ) {
        this(Context.empty(), hours, minutes, seconds, milliSeconds);
    }

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitTime(this);
    }

    @Override
    public Context context() {
        return ctx;
    }

    // TODO: support other time-zones fetched from the scope.
    @Override
    public Expression eval(final Scope scope) {
        final long now = scope.lookup(ctx, Expression.NOW).cast(IntegerExpression.class).getValue();
        final Instant nowInstant = Instant.ofEpochMilli(now);
        final LocalDateTime local = LocalDateTime.ofInstant(nowInstant, ZoneOffset.UTC);

        final int year = local.get(ChronoField.YEAR);
        final int month = local.get(ChronoField.MONTH_OF_YEAR);
        final int dayOfMonth = local.get(ChronoField.DAY_OF_MONTH);

        final Instant instant = LocalDateTime
            .of(year, month, dayOfMonth, hours, minutes, seconds, milliSeconds * 1000000)
            .toInstant(ZoneOffset.UTC);

        return new InstantExpression(ctx, instant);
    }

    @Override
    public String toString() {
        return String.format("{{%02d:%02d:%02d.%03d}}", hours, minutes, seconds, milliSeconds);
    }

    public static TimeExpression parse(final Context c, final String input) {
        final ImmutableList.Builder<Throwable> errors = ImmutableList.builder();

        for (final DateTimeFormatter f : FORMATTERS) {
            final TemporalAccessor accessor;

            try {
                accessor = f.parse(input);
            } catch (final DateTimeException e) {
                errors.add(e);
                continue;
            }

            final int hours = accessor.get(ChronoField.HOUR_OF_DAY);
            final int minutes = accessor.get(ChronoField.MINUTE_OF_HOUR);
            final int seconds = getOrDefault(accessor, ChronoField.SECOND_OF_MINUTE, 0);
            final int milliSeconds = getOrDefault(accessor, ChronoField.MILLI_OF_SECOND, 0);

            return new TimeExpression(c, hours, minutes, seconds, milliSeconds);
        }

        final IllegalArgumentException e =
            new IllegalArgumentException("Invalid instant: " + input);

        errors.build().forEach(e::addSuppressed);

        throw e;
    }

    private static int getOrDefault(
        final TemporalAccessor accessor, final ChronoField field, final int defaultValue
    ) {
        return accessor.isSupported(field) ? accessor.get(field) : defaultValue;
    }

    private static Optional<Integer> getOrEmpty(
        final TemporalAccessor accessor, final ChronoField field
    ) {
        return accessor.isSupported(field) ? Optional.of(accessor.get(field)) : Optional.empty();
    }
}
