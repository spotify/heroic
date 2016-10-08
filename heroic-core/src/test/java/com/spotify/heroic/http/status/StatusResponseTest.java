package com.spotify.heroic.http.status;

import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

public class StatusResponseTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(StatusResponse.class);
    }
}
