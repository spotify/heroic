package com.spotify.heroic.metric;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class QueryTraceTest {
    @Test
    public void passiveJson() throws Exception {
        final ObjectMapper m = new ObjectMapper();
        final String json = "null";
        final QueryTrace trace = QueryTrace.PASSIVE;

        assertEquals(json, m.writeValueAsString(trace));
        assertEquals(trace, m.readValue(json, QueryTrace.class));
    }

    @Test
    public void activeJson() throws Exception {
        final ObjectMapper m = new ObjectMapper();
        final String json = "{\"what\":{\"name\":\"foo\"},\"elapsed\":42,\"children\":[]}";
        final QueryTrace trace =
            new QueryTrace.ActiveTrace(QueryTrace.identifier("foo"), 42L, ImmutableList.of());

        assertEquals(json, m.writeValueAsString(trace));
        assertEquals(trace, m.readValue(json, QueryTrace.class));
    }

    @Test
    public void activeJsonDefault() throws Exception {
        final ObjectMapper m = new ObjectMapper();
        final String json = "{\"what\":{\"name\":\"foo\"},\"elapsed\":42,\"children\":[]}";
        final String serializedJson =
            "{\"what\":{\"name\":\"foo\"},\"elapsed\":42,\"children\":[]}";
        final QueryTrace trace =
            new QueryTrace.ActiveTrace(QueryTrace.identifier("foo"), 42L, ImmutableList.of());

        assertEquals(serializedJson, m.writeValueAsString(trace));
        assertEquals(trace, m.readValue(json, QueryTrace.class));
    }

    @Test
    public void namedWatch() {
        final QueryTrace.Identifier identifier = QueryTrace.identifier("foo");
        final QueryTrace.NamedWatch watch =
            new QueryTrace.ActiveNamedWatch(identifier, Stopwatch.createStarted());
        final QueryTrace trace = watch.end();

        assertTrue(trace instanceof QueryTrace.ActiveTrace);
        final QueryTrace.ActiveTrace activeTrace = (QueryTrace.ActiveTrace) trace;
        assertEquals(identifier, activeTrace.getWhat());
        assertTrue(activeTrace.elapsed() >= 0L);
        assertEquals(ImmutableList.of(), activeTrace.getChildren());
    }

    private QueryTrace tracedMethod(final Tracing tracing) {
        final QueryTrace.NamedWatch parentWatch = tracing.watch(QueryTrace.identifier("parent"));

        final QueryTrace.Joiner parentJoiner = parentWatch.joiner();

        {
            final QueryTrace.NamedWatch childWatch =
                parentWatch.watch(QueryTrace.identifier("one"));
            /* do something that takes time */
            parentJoiner.addChild(childWatch.end());
        }

        {
            final QueryTrace.NamedWatch childWatch =
                parentWatch.watch(QueryTrace.identifier("two"));
            /* do something that takes time */
            parentJoiner.addChild(childWatch.end());
        }

        return parentJoiner.result();
    }

    @Test
    public void tracedMethodEnabled() {
        final QueryTrace trace = tracedMethod(Tracing.fromBoolean(true));
        assertTrue(trace instanceof QueryTrace.ActiveTrace);
        final QueryTrace.ActiveTrace active = (QueryTrace.ActiveTrace) trace;

        assertEquals(QueryTrace.identifier("parent"), active.getWhat());
        assertTrue(active.getElapsed() >= 0);
        assertEquals(2, active.getChildren().size());

        final QueryTrace.ActiveTrace c1 =
            (QueryTrace.ActiveTrace) ((QueryTrace.ActiveTrace) trace).getChildren().get(0);

        assertEquals(QueryTrace.identifier("one"), c1.getWhat());
        assertTrue(c1.getElapsed() >= 0);
        assertEquals(0, c1.getChildren().size());

        final QueryTrace.ActiveTrace c2 =
            (QueryTrace.ActiveTrace) ((QueryTrace.ActiveTrace) trace).getChildren().get(1);

        assertEquals(QueryTrace.identifier("two"), c2.getWhat());
        assertTrue(c2.getElapsed() >= 0);
        assertEquals(0, c2.getChildren().size());
    }

    @Test
    public void tracedMethodDisabled() {
        final QueryTrace trace = tracedMethod(Tracing.fromBoolean(false));
        assertSame(QueryTrace.PASSIVE, trace);
    }
}
