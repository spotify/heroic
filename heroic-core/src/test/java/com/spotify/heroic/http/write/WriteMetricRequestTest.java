package com.spotify.heroic.http.write;

import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

public class WriteMetricRequestTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(WriteMetricRequest.class);
    }
}
