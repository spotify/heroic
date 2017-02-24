package com.spotify.heroic.statistics.semantic;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Before;
import org.junit.Test;

public class MinMaxSlidingTimeReservoirTest {
    private static final int SIZE = 10;
    private static final long STEP = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);
    private static final Snapshot DELEGATE_SNAPSHOT = new Snapshot(new long[]{0, 1, 2});

    private final DeterministicClock clock = new DeterministicClock();

    private Reservoir delegate;
    private MinMaxSlidingTimeReservoir reservoir;

    @Before
    public void setup() {
        delegate = mock(Reservoir.class);
        reservoir = spy(new MinMaxSlidingTimeReservoir(clock, SIZE, STEP, delegate));
        doReturn(DELEGATE_SNAPSHOT).when(delegate).getSnapshot();
    }

    @Test
    public void testMinMaxCalculation() {
        reservoir.update(200L);
        reservoir.update(-200L);

        // cause first updates to go out of range
        clock.set(STEP * (SIZE + 1));

        reservoir.update(100L);
        reservoir.update(-100L);

        verify(delegate).update(100L);
        verify(delegate).update(-100L);

        final Snapshot snapshot = reservoir.getSnapshot();

        assertEquals(100L, snapshot.getMax());
        assertEquals(-100L, snapshot.getMin());

        assertArrayEquals(new long[]{-100L, 1, 100L}, snapshot.getValues());
        assertEquals(-100L, snapshot.getMin());
        assertEquals(100L, snapshot.getMax());
    }

    public static class DeterministicClock extends Clock {
        private final AtomicLong now = new AtomicLong();

        @Override
        public long getTick() {
            return now.get();
        }

        public void set(final long now) {
            this.now.set(now);
        }
    }
}
