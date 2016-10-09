package com.spotify.heroic.aggregation.simple;

import org.junit.Test;

import static com.spotify.heroic.test.LombokDataTest.verifyClassBuilder;

public class BottomKTest {
    @Test
    public void lombokDataTest() {
        verifyClassBuilder(BottomK.class)
            .ignoreGetter("of")
            .valueSupplier(new OfSupplier())
            .verify();
    }
}
