package com.spotify.heroic.metric.datastax.schema;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class AbstractSchemaInstanceTest {
    @Test
    public void testFloatToToken() {
        assertEquals(Long.MAX_VALUE, AbstractSchemaInstance.percentageToToken(1.0f));
        assertEquals(Long.MIN_VALUE, AbstractSchemaInstance.percentageToToken(0.0f));
        assertEquals(0, AbstractSchemaInstance.percentageToToken(0.5f));
    }
}
