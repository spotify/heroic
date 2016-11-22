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

package com.spotify.heroic.metric;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

@AllArgsConstructor
@Data
@Slf4j
public class QueryTrace {
    private final Identifier what;
    private final long elapsed;
    private final List<QueryTrace> children;

    private long preAggregationSampleSize;
    private long numSeries;

    public QueryTrace(final Identifier what, final long elapsed, final List<QueryTrace> children) {
        this.what = what;
        this.elapsed = elapsed;
        this.children = children;
        preAggregationSampleSize = 0;
        numSeries = 0;
        for (QueryTrace child : children) {
            preAggregationSampleSize += child.getPreAggregationSampleSize();
            numSeries += child.getNumSeries();
        }
    }

    public static QueryTrace of(final Identifier what) {
        return new QueryTrace(what, 0L, ImmutableList.of());
    }

    public static QueryTrace of(final Identifier what, final long elapsed) {
        return new QueryTrace(what, elapsed, ImmutableList.of());
    }

    public static QueryTrace of(
        final Identifier what, final long elapsed, final List<QueryTrace> children
    ) {
        return new QueryTrace(what, elapsed, children);
    }

    public static QueryTrace of(
        final Identifier what, final long elapsed, final List<QueryTrace> children,
        final long preAggregationSampleSize, final long numSeries
    ) {
        QueryTrace qt = new QueryTrace(what, elapsed, children, preAggregationSampleSize,
                                       numSeries);
        return qt;
    }

    public void formatTrace(PrintWriter out) {
        formatTrace("", out);
    }

    public void formatTrace(String prefix, PrintWriter out) {
        out.println(prefix + what + " (in " + readableTime(elapsed) + ")");

        for (final QueryTrace child : children) {
            child.formatTrace(prefix + "  ", out);
        }
    }

    public static Identifier identifier(Class<?> where) {
        return new Identifier(where.getName());
    }

    public static Identifier identifier(Class<?> where, String what) {
        return new Identifier(where.getName() + "#" + what);
    }

    public static Identifier identifier(String description) {
        return new Identifier(description);
    }

    private String readableTime(long elapsed) {
        if (elapsed > 1000000000) {
            return String.format("%.3fs", elapsed / 1000000000d);
        }

        if (elapsed > 1000000) {
            return String.format("%.3fms", elapsed / 1000000d);
        }

        if (elapsed > 1000) {
            return String.format("%.3fus", elapsed / 1000d);
        }

        return elapsed + "ns";
    }

    public static Watch watch() {
        return new Watch(Stopwatch.createStarted());
    }

    public static NamedWatch watch(final Identifier what) {
        return new NamedWatch(what, Stopwatch.createStarted());
    }

    @Data
    public static class Watch {
        private final Stopwatch w;

        public QueryTrace end(final Identifier what) {
            return new QueryTrace(what, elapsed(), ImmutableList.of());
        }

        public QueryTrace end(final Identifier what, final QueryTrace child) {
            return new QueryTrace(what, elapsed(), ImmutableList.of(child));
        }

        public QueryTrace end(final Identifier what, final List<QueryTrace> children) {
            return new QueryTrace(what, elapsed(), children);
        }

        public long elapsed() {
            return w.elapsed(TimeUnit.MICROSECONDS);
        }
    }

    @Data
    public static class NamedWatch {
        private final Identifier what;
        private final Stopwatch w;

        public QueryTrace end() {
            return new QueryTrace(what, elapsed(), ImmutableList.of());
        }

        public QueryTrace end(final QueryTrace child) {
            return new QueryTrace(what, elapsed(), ImmutableList.of(child));
        }

        public QueryTrace end(final List<QueryTrace> children) {
            return new QueryTrace(what, elapsed(), children);
        }

        public long elapsed() {
            return w.elapsed(TimeUnit.MICROSECONDS);
        }
    }

    @Data
    public static class Identifier {
        private final String name;

        @JsonCreator
        public Identifier(@JsonProperty("name") String name) {
            this.name = name;
        }

        public Identifier extend(String key) {
            return new Identifier(name + "[" + key + "]");
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
