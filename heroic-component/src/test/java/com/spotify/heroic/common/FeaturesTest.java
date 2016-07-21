package com.spotify.heroic.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FeaturesTest {
    private final ObjectMapper m = new ObjectMapper();

    @Test
    public void basicTest() throws Exception {
        final Features f1 = Features.of(Feature.DISTRIBUTED_AGGREGATIONS);
        final Features f2 = Features.of(Feature.DISTRIBUTED_AGGREGATIONS);
        assertEquals(f1, f1.combine(f2));
        assertTrue(f1.hasFeature(Feature.DISTRIBUTED_AGGREGATIONS));
    }

    @Test
    public void serializationTest() throws Exception {
        final Features f1 = Features.of(Feature.DISTRIBUTED_AGGREGATIONS);
        final String ref = "[\"com.spotify.heroic.distributed_aggregations\"]";

        assertEquals(ref, m.writeValueAsString(f1));
        assertEquals(f1, m.readValue(ref, Features.class));
    }
}
