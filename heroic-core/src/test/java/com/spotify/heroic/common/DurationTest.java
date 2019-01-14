package com.spotify.heroic.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.test.FakeModuleLoader;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class DurationTest {
    private ObjectMapper mapper = FakeModuleLoader.builder().build().json();

    @Test
    public void testShorthandDurationSerializer() throws IOException {
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
    public void testFallbackDurationSerializer() throws IOException {
        final Duration reference = Duration.of(1, TimeUnit.MICROSECONDS);
        final String value = mapper.writeValueAsString(reference);
        assertEquals(reference, mapper.readValue(value, Duration.class));
    }
}
