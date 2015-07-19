package com.spotify.heroic.aggregation;

import org.junit.Test;

/**
 * Tests for a bunch of aggregation archetypes.
 *
 * @author udoprog
 */
public class AggregationTest {
    @Test
    public void testEmptyInChain() {
        Aggregations.chain();
    }
}