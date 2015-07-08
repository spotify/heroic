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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

import lombok.Data;
import lombok.EqualsAndHashCode;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Sampling;
import com.spotify.heroic.model.Statistics;

/**
 * A special aggregation method that is a chain of other aggregation methods.
 *
 * @author udoprog
 */
@Data
@EqualsAndHashCode(of = { "NAME", "chain" })
public class ChainAggregation implements Aggregation {
    private static final ArrayList<Aggregation> EMPTY_AGGREGATORS = new ArrayList<>();
    public static final String NAME = "chain";

    private final List<Aggregation> chain;

    @JsonCreator
    public ChainAggregation(@JsonProperty("chain") List<Aggregation> chain) {
        this.chain = Optional.fromNullable(chain).or(EMPTY_AGGREGATORS);
    }

    @Override
    public Sampling sampling() {
        for (final Aggregation a : chain) {
            final Sampling sampling = a.sampling();

            if (sampling != null)
                return sampling;
        }

        return null;
    }

    @Override
    public long estimate(DateRange range) {
        if (chain.isEmpty())
            return -1;

        return chain.get(chain.size() - 1).estimate(range);
    }

    @Override
    public Aggregation.Session session(Class<?> out, DateRange range) {
        if (chain.isEmpty())
            return new CollectorSession(out);

        final Iterator<Aggregation> iter = chain.iterator();

        Aggregation current = iter.next();

        Class<?> in = current.input();

        if (!matches(out, in))
            throw new IllegalArgumentException("not a valid aggregation chain, step [" + out + " => " + in
                    + "] is not valid");

        out = current.output();

        final Aggregation.Session first = current.session(out, range);
        final List<Aggregation.Session> rest = new ArrayList<Aggregation.Session>();

        while (iter.hasNext()) {
            final Aggregation next = iter.next();

            in = current.input();

            if (!matches(out, in))
                throw new IllegalArgumentException("not a valid aggregation chain, step [" + out + " => " + in
                        + "] is not valid");

            out = current.output();
            rest.add(next.session(out, range));
            current = next;
        }

        return new Session(first, rest, out);
    }

    private boolean matches(Class<?> out, Class<?> in) {
        if (in == null)
            return true;

        if (out == null) {
            if (in != null) {
                throw new IllegalArgumentException("not a valid aggregation chain, step [*anything* => " + in
                        + "] is not valid");
            }
        }

        return in.isAssignableFrom(out);
    }

    @Override
    public List<TraverseState> traverse(List<TraverseState> groups) {
        for (final Aggregation a : chain) {
            groups = a.traverse(groups);
        }

        return groups;
    }

    @Override
    public Class<?> input() {
        if (chain.isEmpty())
            return null;

        for (Aggregation a : chain) {
            final Class<?> in = a.input();

            if (in != null)
                return in;
        }

        return null;
    }

    @Override
    public Class<?> output() {
        if (chain.isEmpty())
            return null;

        final ListIterator<Aggregation> it = chain.listIterator(chain.size());

        while (it.hasPrevious()) {
            final Class<?> out = it.previous().output();

            if (out != null)
                return out;
        }

        return null;
    }

    public static ChainAggregation of(Aggregation... aggregations) {
        return new ChainAggregation(Arrays.asList(aggregations));
    }

    public static List<Aggregation> convertQueries(List<AggregationQuery<?>> aggregators) {
        if (aggregators == null)
            return null;

        final List<Aggregation> result = new ArrayList<>(aggregators.size());

        for (final AggregationQuery<?> aggregation : aggregators)
            result.add(aggregation.build());

        return result;
    }

    private final class Session implements Aggregation.Session {
        private final Aggregation.Session first;
        private final Iterable<Aggregation.Session> rest;
        private final Class<?> output;

        public Session(Aggregation.Session first, Iterable<Aggregation.Session> rest, Class<?> output) {
            this.first = first;
            this.rest = rest;
            this.output = output;
        }

        @Override
        public void update(Aggregation.Group update) {
            first.update(update);
        }

        @Override
        public Aggregation.Result result() {
            final Aggregation.Result firstResult = first.result();
            List<Aggregation.Group> current = firstResult.getResult();
            Statistics.Aggregator statistics = firstResult.getStatistics();

            for (final Aggregation.Session session : rest) {
                for (Aggregation.Group u : current)
                    session.update(u);

                final Aggregation.Result next = session.result();
                current = next.getResult();
                statistics = statistics.merge(next.getStatistics());
            }

            return new Aggregation.Result(current, statistics);
        }

        @Override
        public Class<?> output() {
            return output;
        }
    }

    @Data
    private static final class CollectorSession implements Aggregation.Session {
        private final Class<?> out;

        private final ConcurrentLinkedQueue<Group> input = new ConcurrentLinkedQueue<Group>();

        @Override
        public void update(Group update) {
            input.add(update);
        }

        @Override
        public Result result() {
            final Map<Map<String, String>, Group> result = new HashMap<>();

            for (final Group in : input) {
                final Group group = result.get(in.getGroup());
                result.put(in.getGroup(), group == null ? in : group.merge(in));
            }

            return new Result(new ArrayList<>(result.values()), Statistics.Aggregator.EMPTY);
        }

        @Override
        public Class<?> output() {
            return out;
        }
    }
}
