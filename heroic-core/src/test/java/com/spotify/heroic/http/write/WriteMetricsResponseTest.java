package com.spotify.heroic.http.write;

import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

public class WriteMetricsResponseTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(WriteMetricsResponse.class);
    }
}
