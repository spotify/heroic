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

package com.spotify.heroic;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.TimeUtils;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(QueryDateRange.Absolute.class),
        @JsonSubTypes.Type(QueryDateRange.Relative.class) })
public interface QueryDateRange {
    @Data
    @RequiredArgsConstructor
    @JsonTypeName("absolute")
    public static class Absolute implements QueryDateRange {
        private final long start;
        private final long end;

        @JsonCreator
        public Absolute(@JsonProperty("start") Long start, @JsonProperty("end") Long end) {
            this.start = checkNotNull(start, "start");
            this.end = checkNotNull(end, "end");
        }

        @Override
        public DateRange buildDateRange(final long now) {
            return buildDateRange();
        }

        @Override
        public boolean isEmpty() {
            return end - start <= 0;
        }

        @Override
        public String toDSL() {
            return "(" + start + ", " + end + ")";
        }

        private DateRange buildDateRange() {
            return new DateRange(start, end);
        }
    }

    @Data
    @RequiredArgsConstructor
    @JsonTypeName("relative")
    public static class Relative implements QueryDateRange {
        public static final TimeUnit DEFAULT_UNIT = TimeUnit.DAYS;

        private final TimeUnit unit;
        private final long value;

        @JsonCreator
        public Relative(@JsonProperty("unit") String unit, @JsonProperty("value") Long value) {
            this.unit = TimeUtils.parseTimeUnit(unit).orElse(DEFAULT_UNIT);
            this.value = checkNotNull(value, "value");
        }

        @Override
        public DateRange buildDateRange(final long now) {
            return new DateRange(start(now, value), now);
        }

        @Override
        public boolean isEmpty() {
            return value <= 0;
        }

        @Override
        public String toDSL() {
            return "(" + value + Duration.unitSuffix(unit) + ")";
        }

        private long start(final long now, final long value) {
            return now - TimeUnit.MILLISECONDS.convert(value, unit);
        }
    }

    DateRange buildDateRange(final long now);

    String toDSL();

    @JsonIgnore
    boolean isEmpty();
}
