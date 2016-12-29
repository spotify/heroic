package com.spotify.heroic.metric;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class TracingTest {
    @Test
    public void json() throws Exception {
        final ObjectMapper m = new ObjectMapper();
        assertEquals("\"none\"", m.writeValueAsString(Tracing.NONE));
        assertEquals("\"default\"", m.writeValueAsString(Tracing.DEFAULT));
        assertEquals("\"detailed\"", m.writeValueAsString(Tracing.DETAILED));

        assertEquals(Tracing.NONE, m.readValue("false", Tracing.class));
        assertEquals(Tracing.DEFAULT, m.readValue("true", Tracing.class));
        assertEquals(Tracing.NONE, m.readValue("\"none\"", Tracing.class));
        assertEquals(Tracing.DEFAULT, m.readValue("\"default\"", Tracing.class));
        assertEquals(Tracing.DETAILED, m.readValue("\"detailed\"", Tracing.class));
    }

    @Test
    public void enabled() {
        assertTrue(Tracing.DEFAULT.isEnabled());
        assertTrue(Tracing.DETAILED.isEnabled());
        assertFalse(Tracing.NONE.isEnabled());

        assertTrue(Tracing.NONE.isEnabled(Tracing.NONE));
        assertFalse(Tracing.NONE.isEnabled(Tracing.DEFAULT));
        assertFalse(Tracing.NONE.isEnabled(Tracing.DETAILED));

        assertTrue(Tracing.DEFAULT.isEnabled(Tracing.NONE));
        assertTrue(Tracing.DEFAULT.isEnabled(Tracing.DEFAULT));
        assertFalse(Tracing.DEFAULT.isEnabled(Tracing.DETAILED));

        assertTrue(Tracing.DETAILED.isEnabled(Tracing.NONE));
        assertTrue(Tracing.DETAILED.isEnabled(Tracing.DEFAULT));
        assertTrue(Tracing.DETAILED.isEnabled(Tracing.DETAILED));
    }
}
