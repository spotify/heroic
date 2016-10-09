package com.spotify.heroic.aggregation.simple;

import org.junit.Test;

import static com.spotify.heroic.test.LombokDataTest.verifyClassBuilder;

public class AboveKTest {
    @Test
    public void lombokDataTest() {
        verifyClassBuilder(AboveK.class)
            .ignoreGetter("of")
            .valueSupplier(new OfSupplier())
            .verify();
    }
}
