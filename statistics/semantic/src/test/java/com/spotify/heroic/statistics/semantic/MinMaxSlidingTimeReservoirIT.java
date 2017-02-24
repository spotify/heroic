package com.spotify.heroic.statistics.semantic;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.codahale.metrics.Clock;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import org.junit.Test;

public class MinMaxSlidingTimeReservoirIT {
    private static final int SIZE = 10;
    private static final long STEP = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);
    private static final Snapshot DELEGATE_SNAPSHOT = new Snapshot(new long[]{0, 1, 2});
    private static final int THREAD_COUNT = 4;
    private static final int SAMPLE_SIZE = 100_000;
    private static final int CLOCK_INTERVAL = 10000;
    private static final long ITERATIONS = 10L;

    /**
     * Test many threads updating the reservoir.
     */
    @Test
    public void testManyThreads() throws Exception {
        final ExecutorService pool = Executors.newWorkStealingPool(4);

        // last possible bucket position according to current configuration
        final long lastBucket = THREAD_COUNT * (SAMPLE_SIZE / CLOCK_INTERVAL) - SIZE;

        for (long iteration = 0L; iteration < ITERATIONS; iteration++) {
            final Random random = new Random(0x1234123412341234L + iteration);

            final DeterministicClock clock = new DeterministicClock();

            final Reservoir delegate = new Reservoir() {
                @Override
                public int size() {
                    return 0;
                }

                @Override
                public void update(final long value) {
                }

                @Override
                public Snapshot getSnapshot() {
                    return DELEGATE_SNAPSHOT;
                }
            };

            final MinMaxSlidingTimeReservoir reservoir =
                new MinMaxSlidingTimeReservoir(clock, SIZE, STEP, delegate);

            final LongAccumulator min = new LongAccumulator(Math::min, Long.MAX_VALUE);
            final LongAccumulator max = new LongAccumulator(Math::max, Long.MIN_VALUE);

            final CountDownLatch latch = new CountDownLatch(THREAD_COUNT);

            for (int i = 0; i < THREAD_COUNT; i++) {
                pool.execute(() -> {
                    for (int s = 0; s < SAMPLE_SIZE; s++) {
                        final long sample = random.nextLong();

                        if (s % CLOCK_INTERVAL == 0) {
                            clock.add(STEP);
                        }

                        // check if first bucket according to the clock is after the last possible
                        // bucket. if so, they should be taken into account.
                        if ((reservoir.calculateFirstBucket() + SIZE) > lastBucket) {
                            // start accumulating for reference comparison
                            min.accumulate(sample);
                            max.accumulate(sample);
                        }

                        reservoir.update(sample);
                    }

                    latch.countDown();
                });
            }

            // wait for all threads to complete
            latch.await();

            final Snapshot snapshot = reservoir.getSnapshot();

            assertArrayEquals("expected snapshot for iteration #" + iteration,
                new long[]{min.get(), 1, max.get()}, snapshot.getValues());

            assertEquals("expected max for iteration #" + iteration, max.get(), snapshot.getMax());
            assertEquals("expected min for iteration #" + iteration, min.get(), snapshot.getMin());
        }

        pool.shutdown();
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

        public void add(final long step) {
            this.now.addAndGet(step);
        }
    }
}
