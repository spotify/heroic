package com.spotify.heroic.http.metadata;

import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

public class MetadataQueryBodyTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(MetadataQueryBody.class);
    }
}
