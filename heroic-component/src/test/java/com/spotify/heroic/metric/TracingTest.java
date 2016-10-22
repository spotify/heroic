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
        assertEquals("true", m.writeValueAsString(Tracing.enabled()));
        assertEquals("false", m.writeValueAsString(Tracing.disabled()));

        assertEquals(Tracing.enabled(), m.readValue("true", Tracing.class));
        assertEquals(Tracing.disabled(), m.readValue("false", Tracing.class));
    }

    @Test
    public void enabled() {
        assertTrue(Tracing.enabled().isEnabled());
        assertFalse(Tracing.disabled().isEnabled());
    }

    @Test
    public void staticInstance() {
        assertSame(Tracing.ENABLED, Tracing.enabled());
        assertSame(Tracing.DISABLED, Tracing.disabled());
    }
}
