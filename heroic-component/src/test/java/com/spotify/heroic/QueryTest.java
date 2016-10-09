package com.spotify.heroic;

import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

public class QueryTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(Query.class);
    }
}
