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
import java.util.concurrent.TimeUnit;

import lombok.Data;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.utils.TimeUtils;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = QueryDateRange.Absolute.class, name = "absolute"),
        @JsonSubTypes.Type(value = QueryDateRange.Relative.class, name = "relative") })
public interface QueryDateRange {
    @Data
    public static class Absolute implements QueryDateRange {
        private final long start;
        private final long end;

        @JsonCreator
        public static Absolute create(@JsonProperty("start") Long start, @JsonProperty("end") Long end) {
            if (start == null)
                throw new RuntimeException("'start' is required");

            if (end == null)
                throw new RuntimeException("'end' is required");

            return new Absolute(start, end);
        }

        @Override
        public DateRange buildDateRange() {
            return new DateRange(start, end);
        }
    }

    @Data
    public static class Relative implements QueryDateRange {
        public static final TimeUnit DEFAULT_UNIT = TimeUnit.DAYS;
        public static final long DEFAULT_VALUE = 1;

        private final TimeUnit unit;
        private final long value;

        @JsonCreator
        public static Relative create(@JsonProperty("unit") String unitName, @JsonProperty("value") Long value) {
            final TimeUnit unit = TimeUtils.parseUnitName(unitName, DEFAULT_UNIT);

            if (value == null)
                value = DEFAULT_VALUE;

            return new Relative(unit, value);
        }

        private long start(final Date now) {
            return now.getTime() - TimeUnit.MILLISECONDS.convert(value, unit);
        }

        private long end(final Date now) {
            return now.getTime();
        }

        @Override
        public DateRange buildDateRange() {
            final Date now = new Date();
            return new DateRange(start(now), end(now));
        }
    }

    DateRange buildDateRange();
}