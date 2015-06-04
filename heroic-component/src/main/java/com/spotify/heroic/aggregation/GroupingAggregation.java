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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;

import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.Statistics;

@Data
@EqualsAndHashCode(of = { "of", "each" })
public abstract class GroupingAggregation implements Aggregation {
    private final List<String> of;
    private final Aggregation each;

    protected abstract Map<String, String> key(Map<String, String> input);

    @Override
    public List<TraverseState> traverse(List<TraverseState> states) {
        final Map<Map<String, String>, TraverseState> output = new HashMap<>();

        for (TraverseState s : states) {
            final Map<String, String> k = key(s.getKey());
            TraverseState state = output.get(k);

            if (state == null) {
                state = new TraverseState(k, new HashSet<Series>());
                output.put(k, state);
            }

            state.getSeries().addAll(s.getSeries());
        }

        return new ArrayList<>(output.values());
    }

    @Override
    public long estimate(DateRange range) {
        return each.estimate(range);
    }

    @Override
    public Session session(Class<?> out, DateRange range) {
        return new GroupSession(out, range);
    }

    @Override
    public Sampling sampling() {
        return each.sampling();
    }

    @Override
    public Class<?> input() {
        return each.input();
    }

    @Override
    public Class<?> output() {
        return each.output();
    }

    @RequiredArgsConstructor
    private final class GroupSession implements Session {
        private final ConcurrentHashMap<Map<String, String>, Aggregation.Session> aggregations = new ConcurrentHashMap<>();

        private final Class<?> out;
        private final DateRange range;

        @Override
        public void update(Aggregation.Group group) {
            final Map<String, String> key = key(group.getGroup());
            Aggregation.Session session = aggregations.get(group);

            if (session == null) {
                aggregations.putIfAbsent(key, each.session(out, range));
                session = aggregations.get(key);
            }

            session.update(new Group(key, group.getValues()));
        }

        @Override
        public Result result() {
            final List<Group> groups = new ArrayList<>();

            Statistics.Aggregator statistics = Statistics.Aggregator.EMPTY;

            for (final Map.Entry<Map<String, String>, Session> e : aggregations.entrySet()) {
                Group group = new Group(e.getKey(), Group.EMPTY_VALUES);

                final Result r = e.getValue().result();

                for (final Group g : r.getResult())
                    group = group.merge(g);

                statistics = statistics.merge(r.getStatistics());

                groups.add(group);
            }

            return new Result(groups, statistics);
        }

        @Override
        public Class<?> output() {
            return output();
        }
    }
}