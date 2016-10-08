package com.spotify.heroic.http.cluster;

import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

public class ClusterStatusTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(ClusterStatus.class);
    }
}
