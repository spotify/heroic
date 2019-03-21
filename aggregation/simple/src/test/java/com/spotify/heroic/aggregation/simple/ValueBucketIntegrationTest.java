package com.spotify.heroic.aggregation.simple;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.aggregation.DoubleBucket;
import com.spotify.heroic.metric.Point;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.DoubleBinaryOperator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class ValueBucketIntegrationTest {
    private static final int NCPU = Runtime.getRuntime().availableProcessors();

    private static final Map<String, String> tags = ImmutableMap.of();

    private final double initial;
    private final DoubleBinaryOperator fn;
    private final int threadCount;
    private final long count;
    private final double range;
    private final int iterations;

    public ValueBucketIntegrationTest(double initial, DoubleBinaryOperator fn) {
        this(initial, fn, NCPU, 10000, 1000d, 10);
    }

    private ExecutorService service;

    @java.beans.ConstructorProperties({ "initial", "fn", "threadCount", "count", "range",
                                        "iterations" })
    public ValueBucketIntegrationTest(final double initial, final DoubleBinaryOperator fn,
                                      final int threadCount, final long count, final double range,
                                      final int iterations) {
        this.initial = initial;
        this.fn = fn;
        this.threadCount = threadCount;
        this.count = count;
        this.range = range;
        this.iterations = iterations;
    }

    @Before
    public void setup() {
        service = Executors.newFixedThreadPool(threadCount);
    }

    @After
    public void teardown() {
        service.shutdownNow();
    }

    public abstract Collection<? extends DoubleBucket> buckets();

    @Test(timeout = 10000)
    public void testExpectedValue() throws InterruptedException, ExecutionException {
        final Random rnd = new Random();

        for (final DoubleBucket bucket : buckets()) {
            final List<Future<Void>> futures = new ArrayList<>();

            double expected = initial;

            for (int iteration = 0; iteration < iterations; iteration++) {
                final List<Point> updates = new ArrayList<>();

                double base = iteration * range;

                for (int i = 0; i < count; i++) {
                    final double v1 = base + (rnd.nextDouble() * range);
                    final double v2 = -base + (rnd.nextDouble() * range);

                    updates.add(new Point(0L, v1));
                    updates.add(new Point(0L, v2));

                    expected = fn.applyAsDouble(expected, v1);
                    expected = fn.applyAsDouble(expected, v2);
                }

                for (int thread = 0; thread < threadCount; thread++) {
                    futures.add(service.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            for (final Point d : updates) {
                                bucket.updatePoint(tags, d);
                            }

                            return null;
                        }
                    }));
                }

                for (final Future<Void> f : futures) {
                    f.get();
                }
            }

            assertEquals(bucket.getClass().getSimpleName(), Double.doubleToLongBits(expected),
                Double.doubleToLongBits(bucket.value()));
        }
    }
}
