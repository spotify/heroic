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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.extern.slf4j.Slf4j;

import static org.junit.Assert.assertEquals;

@Slf4j
public abstract class AbstractMetricBackendIT {
    protected abstract Optional<MetricModule> setupModule();

    protected final Series s1 = Series.of("s1", ImmutableMap.of("id", "s1"));
    protected final Series s2 = Series.of("s2", ImmutableMap.of("id", "s2"));
    protected final Series s3 = Series.of("s3", ImmutableMap.of("id", "s3"));

    protected MetricBackend backend;

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
                log.info("Omitting "  + description + " since module is not configured");
            }
        }
    };

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
        // timestamps [1, Long.MAX_VALUE] since we can't fetch 0 (range start is exclusive)
        long timestamp = 1;
        while (timestamp > 0) {
            points.p(timestamp, random.nextDouble());
            timestamp += Math.abs(random.nextLong()) / 100000;
        }

        MetricCollection mc = points.build();
        backend.write(new WriteMetric.Request(s3, mc)).get();

        FetchData.Request request =
            new FetchData.Request(MetricType.POINT, s3, new DateRange(0, Long.MAX_VALUE),
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
