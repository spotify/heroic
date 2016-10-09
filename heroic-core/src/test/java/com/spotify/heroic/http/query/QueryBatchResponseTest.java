package com.spotify.heroic.http.query;

import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

public class QueryBatchResponseTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(QueryBatchResponse.class);
    }
}
