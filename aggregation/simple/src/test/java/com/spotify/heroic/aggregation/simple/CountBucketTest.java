package com.spotify.heroic.aggregation.simple;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.metric.Metric;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

public class CountBucketTest {
    @Test
    public void testCount() {
        final Map<String, String> tags = ImmutableMap.of();
        final CountBucket b = new CountBucket(0);
        final Metric m = mock(Metric.class);
        assertEquals(0L, b.count());
        b.update(tags, m);
        assertEquals(1L, b.count());
    }
}
