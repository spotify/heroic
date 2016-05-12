package com.spotify.heroic.common;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OptionalLimitTest {
    ObjectMapper m;

    @Before
    public void setup() {
        m = new ObjectMapper();
    }

    @Test
    public void testSerialize() throws Exception {
        assertEquals("null", m.writeValueAsString(OptionalLimit.empty()));
        assertEquals("42", m.writeValueAsString(OptionalLimit.of(42)));
    }

    @Test
    public void testDeserialize() throws Exception {
        assertEquals(OptionalLimit.empty(), m.readValue("null", OptionalLimit.class));
        assertEquals(OptionalLimit.of(42), m.readValue("42", OptionalLimit.class));
        assertEquals(OptionalLimit.of(42), m.readValue("42.0", OptionalLimit.class));
    }
}
