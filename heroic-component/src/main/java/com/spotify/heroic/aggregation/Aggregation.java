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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Data;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.Statistics;
import com.spotify.heroic.model.TimeData;

public interface Aggregation {
    @Data
    public static final class Group {
        public static final Map<String, String> EMPTY_GROUP = ImmutableMap.of();
        public static final List<TimeData> EMPTY_VALUES = ImmutableList.<TimeData> of();

        public static final Group EMPTY = new Group(EMPTY_GROUP, EMPTY_VALUES);

        private final Map<String, String> group;
        private final List<? extends TimeData> values;

        public Group merge(Group other) {
            if (this == EMPTY)
                return other;

            final List<TimeData> values = new ArrayList<>(this.values);
            values.addAll(other.values);

            Collections.sort(values);

            return new Group(this.group, values);
        }
    }

    @Data
    public static class Result {
        private final List<Group> result;
        private final Statistics.Aggregator statistics;
    }

    public interface Session {
        /**
         * Stream datapoints into this aggregator.
         *
         * Must be thread-safe.
         *
         * @param datapoints
         */
        public void update(Group update);

        /**
         * Get the result of this aggregator.
         */
        public Result result();

        /**
         * Get type of output from session.
         *
         * @return Output type of session.
         */
        public Class<?> output();
    }

    /**
     * Estimate number of datapoints this aggregation will require.
     *
     * @param range
     *            Range to perform aggregation over.
     * @return Number of datapoints required for aggregation, or {@code -1} if not known.
     */
    public long estimate(DateRange range);

    /**
     * Create an aggregation session.
     *
     * @return
     */
    public Session session(Class<?> in, DateRange range);

    /**
     * Get a hint of the sampling this aggregation uses.
     * 
     * @return The relevant sampling for this aggregation or <code>null</code> if none is available.
     */
    public Sampling sampling();

    @Data
    final static class TraverseState {
        final Map<String, String> key;
        final Set<Series> series;
    }

    /**
     * Traverse the possible aggregations and build the necessary graph out of them.
     */
    public List<TraverseState> traverse(List<TraverseState> states);

    /**
     * Get input type of aggregator.
     */
    public Class<?> input();

    /**
     * Get output type of aggregator.
     */
    public Class<?> output();
}