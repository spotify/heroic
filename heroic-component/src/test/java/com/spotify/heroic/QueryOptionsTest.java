package com.spotify.heroic;

import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

public class QueryOptionsTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(QueryOptions.class);
    }
}
