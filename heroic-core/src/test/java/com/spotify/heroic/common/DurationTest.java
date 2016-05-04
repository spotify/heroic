package com.spotify.heroic.common;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.HeroicMappers;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DurationTest {
    private ObjectMapper mapper = HeroicMappers.json();

    @Test
    public void testShorthandDurationSerializer()
        throws JsonParseException, JsonMappingException, IOException {
        assertEquals(Duration.of(1, TimeUnit.MILLISECONDS),
            mapper.readValue("\"1ms\"", Duration.class));
        assertEquals(Duration.of(2, TimeUnit.MILLISECONDS),
            mapper.readValue("\"2ms\"", Duration.class));
        assertEquals(Duration.of(1, TimeUnit.SECONDS), mapper.readValue("\"1s\"", Duration.class));
        assertEquals(Duration.of(1, TimeUnit.MINUTES), mapper.readValue("\"1m\"", Duration.class));
        assertEquals(Duration.of(1, TimeUnit.HOURS), mapper.readValue("\"1H\"", Duration.class));
        assertEquals(Duration.of(14, TimeUnit.DAYS), mapper.readValue("\"2w\"", Duration.class));
        assertEquals(Duration.of(3600, TimeUnit.MILLISECONDS),
            mapper.readValue("3600", Duration.class));
        assertEquals(Duration.of(3600, TimeUnit.MILLISECONDS),
            mapper.readValue("\"3600\"", Duration.class));
    }

    @Test
    public void testFallbackDurationSerializer()
        throws JsonParseException, JsonMappingException, IOException {
        final Duration reference = Duration.of(1, TimeUnit.MICROSECONDS);
        final String value = mapper.writeValueAsString(reference);
        assertEquals(reference, mapper.readValue(value, Duration.class));
    }
}
