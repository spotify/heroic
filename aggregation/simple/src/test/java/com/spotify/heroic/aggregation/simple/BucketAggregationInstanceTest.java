package com.spotify.heroic.aggregation.simple;

import org.junit.Test;

import static com.spotify.heroic.test.LombokDataTest.verifyClassBuilder;

public class BucketAggregationInstanceTest {
    @Test
    public void testMaxInstance() {
        verifyClassBuilder(MaxInstance.class).verify();
    }

    @Test
    public void testMinInstance() {
        verifyClassBuilder(MinInstance.class).verify();
    }

    @Test
    public void testCountInstance() {
        verifyClassBuilder(CountInstance.class).verify();
    }

    @Test
    public void testSumInstance() {
        verifyClassBuilder(SumInstance.class).verify();
    }

    @Test
    public void testAverageInstance() {
        verifyClassBuilder(AverageInstance.class).verify();
    }

    @Test
    public void testSpreadInstance() {
        verifyClassBuilder(SpreadInstance.class).verify();
    }

    @Test
    public void testQuantileInstance() {
        verifyClassBuilder(QuantileInstance.class).verify();
    }

    @Test
    public void testStdDevInstance() {
        verifyClassBuilder(StdDevInstance.class).verify();
    }

    @Test
    public void testGroupUniqueInstance() {
        verifyClassBuilder(GroupUniqueInstance.class).verify();
    }
}
