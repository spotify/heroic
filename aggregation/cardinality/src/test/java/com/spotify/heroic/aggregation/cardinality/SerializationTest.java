package com.spotify.heroic.aggregation.cardinality;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.test.FakeModuleLoader;
import org.junit.Test;

public class SerializationTest {
    private final FakeModuleLoader m = FakeModuleLoader.builder().module(Module.class).build();
    private final ObjectMapper mapper = m.json();

    @Test
    public void testCardinalityInstance() throws Exception {
        final String json = "{\"type\":\"cardinality\",\"size\":1,\"extent\":2,\"method\":{\"type\":\"exact\",\"includeKey\":false}}";
        final CardinalityInstance aggregation = new CardinalityInstance(1, 2,
            new CardinalityMethod.ExactCardinalityMethod(false));

        assertEquals(json, mapper.writeValueAsString(aggregation));
        assertEquals(aggregation, mapper.readValue(json, AggregationInstance.class));
    }
}
