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

package com.spotify.heroic.http.query;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.TimeUtils;

import lombok.Data;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = QueryDateRange.Absolute.class, name = "absolute"),
        @JsonSubTypes.Type(value = QueryDateRange.Relative.class, name = "relative") })
public interface QueryDateRange {
    @Data
    public static class Absolute implements QueryDateRange {
        private final Optional<Long> start;
        private final Optional<Long> end;

        @JsonCreator
        public Absolute(@JsonProperty("start") Long start, @JsonProperty("end") Long end) {
            this.start = Optional.ofNullable(start);
            this.end = Optional.ofNullable(end);
        }

        @Override
        public Optional<DateRange> buildDateRange() {
            if (start.isPresent() && end.isPresent()) {
                return Optional.of(new DateRange(start.get(), end.get()));
            }

            return Optional.empty();
        }
    }

    @Data
    public static class Relative implements QueryDateRange {
        public static final TimeUnit DEFAULT_UNIT = TimeUnit.DAYS;
        public static final long DEFAULT_VALUE = 1;

        private final TimeUnit unit;
        private final Optional<Long> value;

        @JsonCreator
        public Relative(@JsonProperty("unit") String unit, @JsonProperty("value") Long value) {
            this.unit = TimeUtils.parseTimeUnit(unit).orElse(DEFAULT_UNIT);
            this.value = Optional.ofNullable(value);
        }

        private long start(final Date now, final long value) {
            return now.getTime() - TimeUnit.MILLISECONDS.convert(value, unit);
        }

        private long end(final Date now) {
            return now.getTime();
        }

        @Override
        public Optional<DateRange> buildDateRange() {
            final Date now = new Date();
            return value.map(v -> new DateRange(start(now, v), end(now)));
        }
    }

    Optional<DateRange> buildDateRange();
}