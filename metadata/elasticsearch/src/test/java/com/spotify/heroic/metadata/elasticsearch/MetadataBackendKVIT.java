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

package com.spotify.heroic.metadata.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.elasticsearch.BackendType;
import com.spotify.heroic.elasticsearch.ClientSetup;
import com.spotify.heroic.elasticsearch.Connection;
import com.spotify.heroic.elasticsearch.RateLimitedCache;
import com.spotify.heroic.elasticsearch.StandaloneClientSetup;
import com.spotify.heroic.elasticsearch.index.IndexMapping;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.NotFilter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.async.RetryPolicy;
import eu.toolchain.async.TinyAsync;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.concurrent.Executors;

import static com.spotify.heroic.filter.Filter.and;
import static com.spotify.heroic.filter.Filter.matchKey;
import static com.spotify.heroic.filter.Filter.startsWith;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class MetadataBackendKVIT {
    private MetadataBackendKV metadata;

    private Groups groups = Groups.of("test");
    private AsyncFramework async;

    @Mock
    private MetadataBackendReporter reporter;

    @Mock
    private RateLimitedCache<Pair<String, HashCode>> writeCache;

    private final Series s1 = Series.of("s1", ImmutableMap.of("role", "foo"));
    private final Series s2 = Series.of("s2", ImmutableMap.of("role", "bar"));
    private final Series s3 = Series.of("s3", ImmutableMap.of("role", "baz"));

    private final DateRange range = new DateRange(0L, 0L);

    @Before
    public void setup() throws Exception {
        doReturn(true).when(writeCache).acquire(any());

        async = TinyAsync
            .builder()
            .executor(Executors.newSingleThreadExecutor())
            .scheduler(Executors.newScheduledThreadPool(1))
            .build();

        final IndexMapping index = RotatingIndexMapping.builder().build();
        final ClientSetup setup = StandaloneClientSetup.builder().build();
        final BackendType type = MetadataBackendKV.backendType();

        final Managed<Connection> connection = async.managed(new ManagedSetup<Connection>() {
            @Override
            public AsyncFuture<Connection> construct() throws Exception {
                return async.resolved(new Connection(async, index, setup.setup(), "heroic", type));
            }

            @Override
            public AsyncFuture<Void> destruct(
                final Connection value
            ) throws Exception {
                return value.close();
            }
        });

        metadata = new MetadataBackendKV(groups, reporter, async, connection, writeCache, true);
        metadata
            .start()
            .lazyTransform(v -> async.collect(
                ImmutableList.of(writeSeries(s1, range), writeSeries(s2, range),
                    writeSeries(s3, range))))
            .get();
    }

    @After
    public void teardown() throws Exception {
        metadata.stop().get();
    }

    @Test
    public void testFindSeriesComplex() throws Exception {
        final RangeFilter f = new RangeFilter(and(matchKey("s2"), startsWith("role", "ba")), range,
            OptionalLimit.empty());

        assertEquals(ImmutableSet.of(s2), metadata.findSeries(f).get().getSeries());
    }

    @Test
    public void testFindSeries() throws Exception {
        final RangeFilter f = new RangeFilter(TrueFilter.get(), range, OptionalLimit.empty());
        final FindSeries result = metadata.findSeries(f).get();

        assertEquals(ImmutableSet.of(s1, s2, s3), result.getSeries());
    }

    @Test
    public void testCountSeries() throws Exception {
        final RangeFilter f = new RangeFilter(new NotFilter(new MatchKeyFilter(s2.getKey())), range,
            OptionalLimit.empty());

        final CountSeries result = metadata.countSeries(f).get();
        assertEquals(2L, result.getCount());
    }

    private AsyncFuture<Void> writeSeries(final Series s, final DateRange range) throws Exception {
        final RangeFilter f =
            new RangeFilter(new MatchKeyFilter(s.getKey()), range, OptionalLimit.empty());

        return metadata
            .write(s, range)
            .lazyTransform(v -> async
                .retryUntilResolved(() -> metadata.findSeries(f).directTransform(result -> {
                    if (!result.getSeries().contains(s)) {
                        throw new RuntimeException("Expected to find the written series");
                    }

                    return null;
                }), RetryPolicy.timed(10000, RetryPolicy.exponential(100, 200)))
                .directTransform(r -> null));
    }
}
