package com.spotify.heroic.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FeatureTest {
    private final ObjectMapper m = new ObjectMapper();

    @Test
    public void serializerTest() throws Exception {
        for (final Feature f : Feature.values()) {
            testFeature(Feature.DISTRIBUTED_AGGREGATIONS);
        }
    }

    private void testFeature(final Feature feature) throws Exception {
        assertEquals(String.format("feature (%s) is deserializable", feature), feature,
            m.readValue(String.format("\"%s\"", feature.id()), Feature.class));
        assertEquals(String.format("feature (%s) is serializable", feature),
            String.format("\"%s\"", feature.id()), m.writeValueAsString(feature));
    }
}
