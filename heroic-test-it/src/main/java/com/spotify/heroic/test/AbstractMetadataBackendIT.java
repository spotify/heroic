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
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.GroupMember;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.LoadingComponent;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metadata.WriteMetadata;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.RetryPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;

import static com.spotify.heroic.filter.Filter.and;
import static com.spotify.heroic.filter.Filter.matchKey;
import static com.spotify.heroic.filter.Filter.not;
import static com.spotify.heroic.filter.Filter.startsWith;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractMetadataBackendIT {
    private AsyncFramework async;

    private final Series s1 = Series.of("s1", ImmutableMap.of("role", "foo"));
    private final Series s2 = Series.of("s2", ImmutableMap.of("role", "bar"));
    private final Series s3 = Series.of("s3", ImmutableMap.of("role", "baz"));

    private final DateRange range = new DateRange(0L, 0L);

    private HeroicCoreInstance core;
    private MetadataBackend backend;

    protected abstract MetadataModule setupModule() throws Exception;

    @Before
    public final void abstractSetup() throws Exception {
        final HeroicConfig.Builder fragment = HeroicConfig
            .builder()
            .metadata(MetadataManagerModule.builder().backends(ImmutableList.of(setupModule())));

        core = HeroicCore
            .builder()
            .setupService(false)
            .setupShellServer(false)
            .configFragment(fragment)
            .build()
            .newInstance();

        core.start().get();

        async = core.inject(LoadingComponent::async);

        backend = core
            .inject(c -> c
                .metadataManager()
                .groupSet()
                .inspectAll()
                .stream()
                .map(GroupMember::getMember)
                .findFirst())
            .orElseThrow(() -> new IllegalStateException("Failed to find backend"));

        final List<AsyncFuture<Void>> writes = new ArrayList<>();
        writes.add(writeSeries(backend, s1, range));
        writes.add(writeSeries(backend, s2, range));
        writes.add(writeSeries(backend, s3, range));
        async.collectAndDiscard(writes).get();
    }

    @After
    public final void abstractTeardown() throws Exception {
        core.shutdown().get();
    }

    @Test
    public void findSeriesComplexTest() throws Exception {
        final FindSeries.Request f =
            new FindSeries.Request(and(matchKey("s2"), startsWith("role", "ba")), range,
                OptionalLimit.empty());

        assertEquals(ImmutableSet.of(s2), backend.findSeries(f).get().getSeries());
    }

    @Test
    public void findSeriesTest() throws Exception {
        final FindSeries.Request f =
            new FindSeries.Request(TrueFilter.get(), range, OptionalLimit.empty());

        final FindSeries result = backend.findSeries(f).get();

        assertEquals(ImmutableSet.of(s1, s2, s3), result.getSeries());
    }

    @Test
    public void countSeriesTest() throws Exception {
        final CountSeries.Request f =
            new CountSeries.Request(not(matchKey(s2.getKey())), range, OptionalLimit.empty());

        final CountSeries result = backend.countSeries(f).get();
        assertEquals(2L, result.getCount());
    }

    private AsyncFuture<Void> writeSeries(
        final MetadataBackend metadata, final Series s, final DateRange range
    ) throws Exception {
        final FindSeries.Request f =
            new FindSeries.Request(new MatchKeyFilter(s.getKey()), range, OptionalLimit.empty());

        return metadata
            .write(new WriteMetadata.Request(s, range))
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
