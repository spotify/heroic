package com.spotify.heroic.aggregation.cardinality;

import org.junit.Test;

import static com.spotify.heroic.test.LombokDataTest.verifyClassBuilder;

public class BucketAggregationInstanceTest {
    @Test
    public void testCardinalityInstance() {
        verifyClassBuilder(CardinalityInstance.class).verify();
    }
}
