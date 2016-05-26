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
import lombok.Data;

import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.List;

@Data
@JsonTypeName("date-time")
public class DateTimeExpression implements Expression {
    public static final DateTimeFormatter STRING_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // @formatter:off
    public static final List<DateTimeFormatter> FORMATTERS = ImmutableList.of(
        DateTimeFormatter.ofPattern("yyyy-MM-dd"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS")
    );
    // @formatter:On

    private final Context context;
    private final LocalDateTime dateTime;

    @Override
    public <R> R visit(final Visitor<R> visitor) {
        return visitor.visitDateTime(this);
    }

    // TODO: support other time-zones fetched from the scope.
    @Override
    public Expression eval(final Scope scope) {
        return new InstantExpression(context, dateTime.toInstant(ZoneOffset.UTC));
    }

    @Override
    public String toRepr() {
        return String.format("{%s}", STRING_FORMAT.format(dateTime));
    }

    public static DateTimeExpression parse(final Context c, final String input) {
        final LocalDateTime dateTime = parseLocalDateTime(input);
        return new DateTimeExpression(c, dateTime);
    }

    public static LocalDateTime parseLocalDateTime(final String input) {
        final ImmutableList.Builder<Throwable> errors = ImmutableList.builder();

        for (final DateTimeFormatter f : FORMATTERS) {
            final TemporalAccessor accessor;

            try {
                accessor = f.parse(input);
            } catch (final DateTimeException e) {
                errors.add(e);
                continue;
            }

            final int year = accessor.get(ChronoField.YEAR);
            final int month = accessor.get(ChronoField.MONTH_OF_YEAR);
            final int dayOfMonth = accessor.get(ChronoField.DAY_OF_MONTH);

            final int hour = getOrDefault(accessor, ChronoField.HOUR_OF_DAY, 0);
            final int minute = getOrDefault(accessor, ChronoField.MINUTE_OF_HOUR, 0);
            final int second = getOrDefault(accessor, ChronoField.SECOND_OF_MINUTE, 0);
            final int nanoOfSecond =
                getOrDefault(accessor, ChronoField.MILLI_OF_SECOND, 0) * 1000000;

            return LocalDateTime.of(year, month, dayOfMonth, hour, minute, second, nanoOfSecond);
        }

        final IllegalArgumentException e =
            new IllegalArgumentException("Invalid dateTime: " + input);

        errors.build().forEach(e::addSuppressed);

        throw e;
    }

    private static int getOrDefault(
        final TemporalAccessor accessor, final ChronoField field, final int defaultValue
    ) {
        return accessor.isSupported(field) ? accessor.get(field) : defaultValue;
    }
}
