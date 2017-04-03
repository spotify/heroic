package com.spotify.heroic.aggregation;

import static org.junit.Assert.assertEquals;

import com.spotify.heroic.common.DateRange;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class BucketStrategyTest {
    @Test
    public void testEnd() {
        final BucketStrategy.Mapping mapping =
            BucketStrategy.END.setup(new DateRange(10, 30), 10, 10);

        final Map<Long, BucketStrategy.StartEnd> fromTo = new HashMap<>();

        // underflow
        for (long ts = 0L; ts <= 10L; ts++) {
            fromTo.put(ts, new BucketStrategy.StartEnd(0, 0));
        }

        // first bucket
        for (long ts = 11L; ts <= 20L; ts++) {
            fromTo.put(ts, new BucketStrategy.StartEnd(0, 1));
        }

        // second bucket
        for (long ts = 21L; ts <= 30L; ts++) {
            fromTo.put(ts, new BucketStrategy.StartEnd(1, 2));
        }

        // overflow
        for (long ts = 31L; ts <= 40L; ts++) {
            fromTo.put(ts, new BucketStrategy.StartEnd(2, 2));
        }

        for (final Map.Entry<Long, BucketStrategy.StartEnd> e : fromTo.entrySet()) {
            assertEquals("Expected same mapping for timestamp " + e.getKey(), e.getValue(),
                mapping.map(e.getKey()));
        }
    }
}
