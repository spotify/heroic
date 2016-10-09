package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.Payload;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.Spread;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AnyBucketTest {
    @Test
    public void forwarding() {
        final AnyBucket any = mock(AnyBucket.class, Mockito.CALLS_REAL_METHODS);
        final Map<String, String> tags = ImmutableMap.of();

        {
            final Event event = mock(Event.class);
            any.updateEvent(tags, event);
            verify(any).update(tags, event);
        }

        {
            final Point point = mock(Point.class);
            any.updatePoint(tags, point);
            verify(any).update(tags, point);
        }

        {
            final Spread spread = mock(Spread.class);
            any.updateSpread(tags, spread);
            verify(any).update(tags, spread);
        }

        {
            final MetricGroup group = mock(MetricGroup.class);
            any.updateGroup(tags, group);
            verify(any).update(tags, group);
        }

        {
            final Payload payload = mock(Payload.class);
            any.updatePayload(tags, payload);
            verify(any).update(tags, payload);
        }
    }
}
