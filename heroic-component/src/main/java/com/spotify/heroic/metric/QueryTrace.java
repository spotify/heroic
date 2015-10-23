package com.spotify.heroic.metric;

import java.io.PrintWriter;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.QueryTrace.Identifier;

import lombok.Data;

@Data
public class QueryTrace {
    private final Identifier what;
    private final long elapsed;
    private final List<QueryTrace> children;

    @JsonCreator
    public QueryTrace(@JsonProperty("what") final QueryTrace.Identifier what, @JsonProperty("elapsed") final long elapsed,
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