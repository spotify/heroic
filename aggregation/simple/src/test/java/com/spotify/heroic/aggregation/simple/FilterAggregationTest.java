package com.spotify.heroic.aggregation.simple;

import org.junit.Test;

import static com.spotify.heroic.test.LombokDataTest.verifyClassBuilder;

public class FilterAggregationTest {
    @Test
    public void testAboveKInstance() {
        verifyClassBuilder(AboveKInstance.class).checkGetters(false).verify();
    }

    @Test
    public void testBelowKInstance() {
        verifyClassBuilder(BelowKInstance.class).checkGetters(false).verify();
    }

    @Test
    public void testBottomKInstance() {
        verifyClassBuilder(BottomKInstance.class).checkGetters(false).verify();
    }

    @Test
    public void testTopKInstance() {
        verifyClassBuilder(TopKInstance.class).checkGetters(false).verify();
    }
}
