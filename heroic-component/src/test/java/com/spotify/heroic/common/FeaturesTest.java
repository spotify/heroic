package com.spotify.heroic.common;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSortedSet;
import org.junit.Test;

public class FeaturesTest {
    private final ObjectMapper m = new ObjectMapper();

    @Test
    public void serializationTest() throws Exception {
        final Features f1 = new Features(ImmutableSortedSet.of(Feature.DISTRIBUTED_AGGREGATIONS));
        final String ref = "[\"com.spotify.heroic.distributed_aggregations\"]";

        assertEquals(ref, m.writeValueAsString(f1));
        assertEquals(f1, m.readValue(ref, Features.class));
    }
}
