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

import java.io.PrintWriter;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import lombok.Data;

@Data
public class QueryTrace {
    private final Identifier what;
    private final long elapsed;
    private final List<QueryTrace> children;

    @JsonCreator
    public QueryTrace(@JsonProperty("what") final QueryTrace.Identifier what,
            @JsonProperty("elapsed") final long elapsed,
            @JsonProperty("children") final List<QueryTrace> children) {
        this.what = what;
        this.elapsed = elapsed;
        this.children = children;
    }

    public QueryTrace(final QueryTrace.Identifier what) {
        this(what, 0L, ImmutableList.of());
    }

    public QueryTrace(final QueryTrace.Identifier what, final long elapsed) {
        this(what, elapsed, ImmutableList.of());
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
