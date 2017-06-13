/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.test;

import static com.spotify.heroic.test.Data.events;
import static com.spotify.heroic.test.Data.points;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.GroupMember;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.WriteMetric;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

@Slf4j
public abstract class AbstractMetricBackendIT {
    protected final Series s1 = Series.of("s1", ImmutableMap.of("id", "s1"));
    protected final Series s2 = Series.of("s2", ImmutableMap.of("id", "s2"));
    protected final Series s3 = Series.of("s3", ImmutableMap.of("id", "s3"));

    public static final Map<String, String> EVENT = ImmutableMap.of();

    protected MetricBackend backend;

    /* For backends which  currently does not implement period edge cases correctly,
     * See: https://github.com/spotify/heroic/pull/208 */
    protected boolean brokenSegmentsPr208 = false;
    protected boolean eventSupport = false;
    protected Optional<Integer> maxBatchSize = Optional.empty();

    @Rule
    public TestRule setupBackend = (base, description) -> new Statement() {
        @Override
        public void evaluate() throws Throwable {
            Optional<MetricModule> module = setupModule();
            if (module.isPresent()) {
                final MetricManagerModule.Builder metric =
                    MetricManagerModule.builder().backends(ImmutableList.of(module.get()));

                final HeroicConfig.Builder fragment = HeroicConfig.builder().metrics(metric);

                final HeroicCoreInstance core = HeroicCore
                    .builder()
                    .setupShellServer(false)
                    .setupService(false)
                    .configFragment(fragment)
                    .build()
                    .newInstance();

                core.start().get();

                backend = core
                    .inject(c -> c
                        .metricManager()
                        .groupSet()
                        .inspectAll()
                        .stream()
                        .map(GroupMember::getMember)
                        .findFirst())
                    .orElseThrow(() -> new IllegalStateException("Failed to find backend"));
                base.evaluate();
                core.shutdown().get();
            } else {
                log.info("Omitting " + description + " since module is not configured");
            }
        }
    };

    @Before
    public void setup() {
        setupSupport();
    }

    protected abstract Optional<MetricModule> setupModule();

    protected Optional<Long> period() {
        return Optional.empty();
    }

    /**
     * Setup backend-specific support.
     */
    protected void setupSupport() {
    }

    @Test
    public void testInterval() throws Exception {
        newCase()
            .input(99L, 100L, 101L, 199L, 200L, 201L)
            .expect(101L, 199L, 200L)
            .forEach((input, expected) -> {
                verifyReadWrite(input, expected, new DateRange(100L, 200L));
            });
    }

    /**
     * Some backends have optimized code for writing a single sample.
     */
    @Test
    public void testOne() throws Exception {
        newCase().input(100L).expect(100L).forEach((input, expected) -> {
            verifyReadWrite(input, expected, new DateRange(99L, 100L));
        });
    }

    /**
     * Some backends have limits on how many metrics should be written in a single request.
     *
     * This test case creates a dense pool of metrics after a given max batch size to make sure that
     * they can be written and read out.
     */
    @Test
    public void testMaxBatchSize() throws Exception {
        assumeTrue("max batch size", maxBatchSize.isPresent());
        final int maxBatchSize = this.maxBatchSize.get();

        newCase().denseStart(100).dense(maxBatchSize * 4).forEach((input, expected) -> {
            verifyReadWrite(input, expected, new DateRange(99L, 100L + (maxBatchSize * 4)));
        });
    }

    /**
     * This test is run if the specific test overrides the {@link #period()} method and returns a
     * value (not {@link java.util.Optional#empty()}.
     * <p>
     * This value will be considered the period of the metric backend, which indicates at what
     * distance the backend will split samples up into distinct storage parts. These would typically
     * make up edge cases that this test case attempts to read and write.
     */
    @Test
    public void testPeriod() throws Exception {
        final Optional<Long> maybePeriod = period();
        assumeTrue("period is present", maybePeriod.isPresent());

        final long count = 5;
        final long period = maybePeriod.get();

        final Points points = points();

        // seed data just at the edges of the period
        for (int i = 1; i < count; i++) {
            long first = i * period;

            if (brokenSegmentsPr208) {
                // due to off-by-one, points exactly on the period are invisible.
                first += 1;
            }

            points.p(first, (double) i);
            points.p((i + 1) * period - 1, (double) i + .5D);
        }

        final MetricCollection written = points.build();

        verifyReadWrite(written, written, new DateRange(period - 1, (period + 1) * count));
    }

    private void verifyReadWrite(
        final MetricCollection input, final MetricCollection expected, final DateRange range
    ) throws Exception {
        backend.write(new WriteMetric.Request(s1, input)).get();

        FetchData data = backend
            .fetch(new FetchData.Request(expected.getType(), s1, range,
                QueryOptions.builder().build()), FetchQuotaWatcher.NO_QUOTA)
            .get();

        assertEquals(ImmutableSet.of(expected), ImmutableSet.copyOf(data.getGroups()));
    }

    private TestCase newCase() {
        return new TestCase();
    }

    @lombok.Data
    private class TestCase {
        private Optional<Integer> denseStart = Optional.empty();
        private Optional<Integer> dense = Optional.empty();
        private final List<Long> input = new ArrayList<>();
        private final List<Long> expected = new ArrayList<>();

        TestCase denseStart(final int denseStart) {
            this.denseStart = Optional.of(denseStart);
            return this;
        }

        TestCase dense(final int dense) {
            this.dense = Optional.of(dense);
            return this;
        }

        TestCase input(final long... inputs) {
            for (final long input : inputs) {
                this.input.add(input);
            }

            return this;
        }

        TestCase expect(final long... expected) {
            for (final long expect : expected) {
                this.expected.add(expect);
            }

            return this;
        }

        void forEach(final ThrowingBiConsumer<MetricCollection, MetricCollection> consumer)
            throws Exception {
            // test for events, if supported
            if (eventSupport) {
                final Events input = events();
                final Events expected = events();

                inputStream().forEach(t -> input.e(t, EVENT));
                expectedStream().forEach(t -> expected.e(t, EVENT));

                consumer.accept(input.build(), expected.build());
            }

            // test for points
            {
                final Points input = points();
                final Points expected = points();

                inputStream().forEach(t -> input.p(t, 42D));
                expectedStream().forEach(t -> expected.p(t, 42D));

                consumer.accept(input.build(), expected.build());
            }
        }

        private Stream<Long> inputStream() {
            return Stream.concat(this.input.stream(), this.denseStream());
        }

        private Stream<Long> expectedStream() {
            return Stream.concat(this.expected.stream(), this.denseStream());
        }

        private Stream<Long> denseStream() {
            return dense.map(d -> {
                final Stream.Builder<Long> builder = Stream.builder();

                final int start = denseStart.orElse(0);

                for (int t = 0; t < d; t++) {
                    builder.add((long) (t + start));
                }

                return builder.build();
            }).orElseGet(Stream::empty);
        }
    }

    @FunctionalInterface
    interface ThrowingBiConsumer<A, B> {
        void accept(final A a, final B b) throws Exception;
    }

    @Test
    public void testWriteAndFetchOne() throws Exception {
        final MetricCollection points = Data.points().p(100000L, 42D).build();
        backend.write(new WriteMetric.Request(s1, points)).get();

        FetchData.Request request =
            new FetchData.Request(MetricType.POINT, s1, new DateRange(10000L, 200000L),
                QueryOptions.builder().build());

        assertEqualMetrics(points, fetchMetrics(request, true));
        assertEqualMetrics(points, fetchMetrics(request, false));
    }

    @Test
    public void testWriteAndFetchMultipleFetches() throws Exception {
        int fetchSize = 20;

        Points points = Data.points();
        for (int i = 0; i < 10 * fetchSize; i++) {
            points.p(100000L + i, 42D);
        }

        final MetricCollection mc = points.build();
        backend.write(new WriteMetric.Request(s2, mc)).get();

        FetchData.Request request =
            new FetchData.Request(MetricType.POINT, s2, new DateRange(10000L, 200000L),
                QueryOptions.builder().fetchSize(fetchSize).build());

        assertEqualMetrics(mc, fetchMetrics(request, true));
    }

    @Test
    public void testWriteAndFetchLongSeries() throws Exception {
        Random random = new Random(1);

        Points points = Data.points();

        long timestamp = 1;
        long maxTimestamp = 1000000000000L;
        // timestamps [1, maxTimestamp] since we can't fetch 0 (range start is exclusive)
        while (timestamp < maxTimestamp) {
            points.p(timestamp, random.nextDouble());
            timestamp += Math.abs(random.nextInt(100000000));
        }
        points.p(maxTimestamp, random.nextDouble());

        MetricCollection mc = points.build();
        backend.write(new WriteMetric.Request(s3, mc)).get();

        FetchData.Request request =
            new FetchData.Request(MetricType.POINT, s3, new DateRange(0, maxTimestamp),
                QueryOptions.builder().build());

        assertEqualMetrics(mc, fetchMetrics(request, true));
        assertEqualMetrics(mc, fetchMetrics(request, false));
    }

    private List<MetricCollection> fetchMetrics(FetchData.Request request, boolean slicedFetch)
        throws Exception {
        if (slicedFetch) {
            List<MetricCollection> fetchedMetrics = Collections.synchronizedList(new ArrayList<>());
            backend.fetch(request, FetchQuotaWatcher.NO_QUOTA, fetchedMetrics::add).get();
            return fetchedMetrics;
        } else {
            return backend.fetch(request, FetchQuotaWatcher.NO_QUOTA).get().getGroups();
        }
    }

    private static void assertEqualMetrics(
        MetricCollection expected, List<MetricCollection> actual
    ) {
        Stream<MetricType> types = actual.stream().map(MetricCollection::getType);
        assertEquals(ImmutableSet.of(expected.getType()), types.collect(Collectors.toSet()));
        actual
            .stream()
            .flatMap(mc -> mc.getData().stream())
            .sorted(Metric.comparator())
            .forEach(new Consumer<Metric>() {
                int i;

                @Override
                public void accept(final Metric metric) {
                    assertEquals(expected.getData().get(i), metric);
                    i++;
                }
            });
    }

}
