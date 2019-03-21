package com.spotify.heroic.aggregation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.HeroicMappers;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.grammar.QueryParser;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.spotify.heroic.test.Resources.openResource;
import static org.junit.Assert.assertEquals;

public class SamplingQueryDeserializationTest {
    private final ObjectMapper m = HeroicMappers.json(Mockito.mock(QueryParser.class));

    @Test
    public void deserializationTest() throws Exception {
        final Duration d = Duration.of(10, TimeUnit.MINUTES);

        try (final InputStream in = openResource(getClass(), "SamplingQuery.1.json")) {
            assertEquals(new SamplingQuery(d, null),
                m.readValue(in, SamplingQuery.class));
        }

        try (final InputStream in = openResource(getClass(), "SamplingQuery.2.json")) {
            assertEquals(new SamplingQuery(null, d),
                m.readValue(in, SamplingQuery.class));
        }
    }
}
