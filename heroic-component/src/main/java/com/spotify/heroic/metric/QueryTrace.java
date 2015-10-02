package com.spotify.heroic.metric;

import java.io.PrintWriter;
import java.util.List;

import com.google.common.collect.ImmutableList;

import lombok.Data;
import lombok.RequiredArgsConstructor;

@Data
@RequiredArgsConstructor
public class QueryTrace {
    private final String what;
    private final long elapsed;
    private final List<QueryTrace> children;

    public QueryTrace(final String what, final long micros) {
        this(what, micros, ImmutableList.of());
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
}