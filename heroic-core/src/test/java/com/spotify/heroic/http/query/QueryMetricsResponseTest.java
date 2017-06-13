package com.spotify.heroic.http.query;

import com.spotify.heroic.metric.QueryMetricsResponse;
import com.spotify.heroic.test.LombokDataTest;
import org.junit.Test;

public class QueryMetricsResponseTest {
    @Test
    public void lombokDataTest() {
        LombokDataTest.verifyClass(QueryMetricsResponse.class);
    }
}
