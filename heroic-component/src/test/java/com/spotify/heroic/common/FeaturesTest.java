package com.spotify.heroic.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class FeaturesTest {
    private final ObjectMapper m = new ObjectMapper();

    @Test
    public void serializationTest() throws Exception {
        final Features f1 = new Features(ImmutableSet.of(Feature.DISTRIBUTED_AGGREGATIONS));
        final String ref = "[\"com.spotify.heroic.distributed_aggregations\"]";

        assertEquals(ref, m.writeValueAsString(f1));
        assertEquals(f1, m.readValue(ref, Features.class));
    }
}
