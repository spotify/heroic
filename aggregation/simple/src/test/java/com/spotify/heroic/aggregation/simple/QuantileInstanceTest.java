package com.spotify.heroic.aggregation.simple;

import org.junit.Test;

import static com.spotify.heroic.test.LombokDataTest.verifyClassBuilder;

public class QuantileInstanceTest {
    @Test
    public void lombokDataTest() {
        verifyClassBuilder(QuantileInstance.class).verify();
    }
}
