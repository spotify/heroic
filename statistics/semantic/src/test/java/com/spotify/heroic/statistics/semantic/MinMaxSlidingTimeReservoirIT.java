package com.spotify.heroic.statistics.semantic;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.codahale.metrics.Clock;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Reservoir;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformSnapshot;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import org.junit.Test;

public class MinMaxSlidingTimeReservoirIT {
    private static final int SIZE = 10;
    private static final long STEP = TimeUnit.NANOSECONDS.convert(1, TimeUnit.SECONDS);
    private static final Snapshot DELEGATE_SNAPSHOT = new UniformSnapshot(new long[]{ 0, 1, 2});
    private static final int THREAD_COUNT = 4;
    private static final int SAMPLE_SIZE = 100_000;
    private static final int CLOCK_INTERVAL = 10000;
    private static final long ITERATIONS = 10L;
    private static final long VALUE_RANGE = 1000_000;
    private static final long MAX_VALUE = VALUE_RANGE * 11;
    private static final long MIN_VALUE = -VALUE_RANGE * 11;

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
                new MinMaxSlidingTimeReservoir(clock, SIZE, STEP, TimeUnit.NANOSECONDS, delegate);

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

    @Test
    public void testBasicStatisticsSlowRate() throws Exception {
        final DeterministicClock clock = new DeterministicClock();

        int iterations = 10;
        int numSamples = 100;

        final Reservoir delegate = new ExponentiallyDecayingReservoir(1028, 0.015, clock);
        final MinMaxSlidingTimeReservoir reservoir =
            new MinMaxSlidingTimeReservoir(clock, SIZE, STEP, TimeUnit.NANOSECONDS, delegate);
        long exactValues[] = new long[(numSamples + 2) * iterations];
        int i = 0;

        for (int iteration = 0; iteration < iterations; iteration++) {
            long maxPos = ThreadLocalRandom.current().nextInt(0, numSamples);
            long minPos = ThreadLocalRandom.current().nextInt(0, numSamples);
            for (long pos = 0; pos < numSamples; pos++) {
                long val = ThreadLocalRandom.current().nextLong(-VALUE_RANGE, VALUE_RANGE);
                reservoir.update(val);
                exactValues[i] = val;
                i++;
                if (pos == maxPos) {
                    reservoir.update(MAX_VALUE);
                    exactValues[i] = MAX_VALUE;
                    i++;
                }
                if (pos == minPos) {
                    reservoir.update(MIN_VALUE);
                    exactValues[i] = MIN_VALUE;
                    i++;
                }
            }

            final Snapshot snapshot = reservoir.getSnapshot();

            assertEquals(MAX_VALUE, snapshot.getMax());
            assertEquals(MIN_VALUE, snapshot.getMin());

            long expectedValues[] = Arrays.copyOf(exactValues, i);
            Arrays.sort(expectedValues);
            long reservoirValues[] =
                Arrays.copyOf(snapshot.getValues(), snapshot.getValues().length);
            Arrays.sort(reservoirValues);

            assertArrayEquals(expectedValues, reservoirValues);
        }
    }

    @Test
    public void testBasicStatisticsHighRate() throws Exception {
        final DeterministicClock clock = new DeterministicClock();

        int iterations = 2;
        for (int iteration = 0; iteration < iterations; iteration++) {
            final Reservoir delegate = new ExponentiallyDecayingReservoir(1028, 0.015, clock);
            final MinMaxSlidingTimeReservoir reservoir =
                new MinMaxSlidingTimeReservoir(clock, SIZE, STEP, TimeUnit.NANOSECONDS, delegate);

            int numSamples = 1000000;
            int clockInterval = numSamples / SIZE;
            long exactValues[] = new long[numSamples + 2];
            long maxPos = ThreadLocalRandom.current().nextInt(0, numSamples);
            long minPos = ThreadLocalRandom.current().nextInt(0, numSamples);
            int i = 0;
            for (long pos = 0; pos < numSamples; pos++) {
                if (pos > 0 && pos % clockInterval == 0) {
                    clock.add(STEP);
                }
                long val = ThreadLocalRandom.current().nextLong(-VALUE_RANGE, VALUE_RANGE);
                reservoir.update(val);
                exactValues[i] = val;
                i++;
                // Insert an extreme max / min value at a random point in the reservoir
                if (pos == maxPos) {
                    reservoir.update(MAX_VALUE);
                    exactValues[i] = MAX_VALUE;
                    i++;
                }
                if (pos == minPos) {
                    reservoir.update(MIN_VALUE);
                    exactValues[i] = MIN_VALUE;
                    i++;
                }
            }

            final Snapshot snapshot = reservoir.getSnapshot();

            assertEquals("Max value", MAX_VALUE, snapshot.getMax());
            assertEquals("Min value", MIN_VALUE, snapshot.getMin());

            final long actualValues[] = Arrays.copyOf(snapshot.getValues(), snapshot.getValues().length);
            assertTrue("Reservoir contains values", actualValues.length > 1000);

            final Set<Long> exactValueSet = new HashSet<>();
            for (i = 0; i < exactValues.length; i++) {
                exactValueSet.add(exactValues[i]);
            }
            assertTrue("Only known values in the reservoir", Arrays
                .stream(actualValues)
                .filter(value -> !exactValueSet.contains(value))
                .count() == 0);

            final long zeroValueRange = (VALUE_RANGE * 10) / 100;
            assertThat("Mean value is within 10% error rate of 0", (long) snapshot.getMean(),
                allOf(greaterThan(-zeroValueRange), lessThan(zeroValueRange)));

            final long stdDev = (long) snapshot.getStdDev();
            assertThat("Mean deviation is more than 40% of value range", stdDev,
                greaterThan((VALUE_RANGE * 40) / 100));
            assertThat("Mean deviation is less than the max value range", stdDev,
                lessThan(MAX_VALUE));

            final Snapshot snapshot2 = reservoir.getSnapshot();
            assertArrayEquals("Two calls to get snapshot results in same data",
                snapshot.getValues(), snapshot2.getValues());
        }
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
