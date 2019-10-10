package com.spotify.heroic.aggregation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import java.util.ArrayList;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mockito;

public class AnyBucketTest {
    @Test
    public void forwarding() {
        final AnyBucket any = mock(AnyBucket.class, Mockito.CALLS_REAL_METHODS);
        final Map<String, String> tags = ImmutableMap.of();

        {
            final Point point = new Point(0, 0);
            any.updatePoint(tags, point);
            verify(any).update(tags, point);
        }

        {
            final Spread spread = new Spread(0, 0, 0, 0, 0, 0);
            any.updateSpread(tags, spread);
            verify(any).update(tags, spread);
        }

        {
            final MetricGroup group = new MetricGroup(0, new ArrayList<>());
            any.updateGroup(tags, group);
            verify(any).update(tags, group);
        }

        {
            final Payload payload = new Payload(0, "".getBytes());
            any.updatePayload(tags, payload);
            verify(any).update(tags, payload);
        }
    }
}
