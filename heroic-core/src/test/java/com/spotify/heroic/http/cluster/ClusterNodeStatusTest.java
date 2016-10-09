package com.spotify.heroic.http.cluster;

import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

public class ClusterNodeStatusTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(ClusterNodeStatus.class);
    }
}
