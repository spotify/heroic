package com.spotify.heroic.common;

import com.google.common.collect.ImmutableSet;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FeatureSetTest {
    private final ObjectMapper m = new ObjectMapper();

    @Test
    public void of() {
        assertEquals(
            new FeatureSet(ImmutableSet.of(Feature.DISTRIBUTED_AGGREGATIONS), ImmutableSet.of()),
            FeatureSet.of(Feature.DISTRIBUTED_AGGREGATIONS));
    }

    @Test
    public void empty() {
        assertEquals(new FeatureSet(ImmutableSet.of(), ImmutableSet.of()), FeatureSet.empty());
    }

    @Test
    public void applySetTest() {
        final FeatureSet s1 =
            new FeatureSet(ImmutableSet.of(Feature.DISTRIBUTED_AGGREGATIONS), ImmutableSet.of());

        final FeatureSet s2 = new FeatureSet(ImmutableSet.of(Feature.SHIFT_RANGE),
            ImmutableSet.of(Feature.DISTRIBUTED_AGGREGATIONS));

        assertEquals(Features.of(Feature.DISTRIBUTED_AGGREGATIONS), Features.empty().applySet(s1));
        assertEquals(Features.of(Feature.SHIFT_RANGE), Features.empty().applySet(s1).applySet(s2));
        assertEquals(
            new FeatureSet(ImmutableSet.of(Feature.SHIFT_RANGE, Feature.DISTRIBUTED_AGGREGATIONS),
                ImmutableSet.of(Feature.DISTRIBUTED_AGGREGATIONS)), s1.combine(s2));
    }

    @Test
    public void serializationTest() throws Exception {
        final FeatureSet f = new FeatureSet(ImmutableSet.of(Feature.DISTRIBUTED_AGGREGATIONS),
            ImmutableSet.of(Feature.SHIFT_RANGE));
        final String ref = "[" + "\"com.spotify.heroic.distributed_aggregations\"," +
            "\"-com.spotify.heroic.shift_range\"" + "]";

        assertEquals(ref, m.writeValueAsString(f));
        assertEquals(f, m.readValue(ref, FeatureSet.class));
    }
}
