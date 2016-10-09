package com.spotify.heroic.aggregation.simple;

import org.junit.Test;

import static com.spotify.heroic.test.LombokDataTest.verifyClassBuilder;

public class TopKTest {
    @Test
    public void lombokDataTest() {
        verifyClassBuilder(TopK.class).ignoreGetter("of").valueSupplier(new OfSupplier()).verify();
    }
}
