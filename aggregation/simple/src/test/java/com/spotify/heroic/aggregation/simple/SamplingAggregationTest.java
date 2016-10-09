package com.spotify.heroic.aggregation.simple;

import org.junit.Test;

import static com.spotify.heroic.test.LombokDataTest.verifyClassBuilder;

public class SamplingAggregationTest {
    @Test
    public void testMin() {
        verifyClassBuilder(Min.class)
            .checkGetters(false)
            .valueSupplier(new SamplingSupplier())
            .verify();
    }

    @Test
    public void testMax() {
        verifyClassBuilder(Max.class)
            .checkGetters(false)
            .valueSupplier(new SamplingSupplier())
            .verify();
    }

    @Test
    public void testAverage() {
        verifyClassBuilder(Average.class)
            .checkGetters(false)
            .valueSupplier(new SamplingSupplier())
            .verify();
    }

    @Test
    public void testSum() {
        verifyClassBuilder(Sum.class)
            .checkGetters(false)
            .valueSupplier(new SamplingSupplier())
            .verify();
    }

    @Test
    public void testGroupUnique() {
        verifyClassBuilder(GroupUnique.class)
            .checkGetters(false)
            .valueSupplier(new SamplingSupplier())
            .verify();
    }

    @Test
    public void testStdDev() {
        verifyClassBuilder(StdDev.class)
            .checkGetters(false)
            .valueSupplier(new SamplingSupplier())
            .verify();
    }

    @Test
    public void testCount() {
        verifyClassBuilder(Count.class)
            .checkGetters(false)
            .valueSupplier(new SamplingSupplier())
            .verify();
    }

    @Test
    public void testSpread() {
        verifyClassBuilder(Spread.class)
            .checkGetters(false)
            .valueSupplier(new SamplingSupplier())
            .verify();
    }
}
