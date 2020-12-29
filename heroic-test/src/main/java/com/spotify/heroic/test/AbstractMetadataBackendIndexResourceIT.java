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

import static com.spotify.heroic.filter.Filter.and;
import static com.spotify.heroic.filter.Filter.hasTag;
import static com.spotify.heroic.filter.Filter.matchKey;
import static com.spotify.heroic.filter.Filter.matchTag;
import static com.spotify.heroic.filter.Filter.not;
import static com.spotify.heroic.filter.Filter.or;
import static com.spotify.heroic.filter.Filter.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;
import static org.junit.Assume.assumeTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.GroupMember;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.LoadingComponent;
import com.spotify.heroic.filter.FalseFilter;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindSeriesIds;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metadata.WriteMetadata;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.RetryPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractMetadataBackendIndexResourceIT {
    protected final String testName = "heroic-it-" + UUID.randomUUID().toString();

    private AsyncFramework async;

    protected final int numSeries = 3;

    protected final Series s1 = Series.of(
        "s1",
        ImmutableMap.of("role", "foo"),
        ImmutableMap.of("podname", "foo-bar-123")
    );
    protected final Series s2 = Series.of(
        "s2",
        ImmutableMap.of("role", "bar"),
        ImmutableMap.of("podname", "foo-bar-456")
    );
    protected final Series s3 = Series.of(
        "s3",
        ImmutableMap.of("role", "bar"),
        ImmutableMap.of("podname", "foo-bar-789")
    );

    protected final DateRange range = new DateRange(0L, 0L);

    private HeroicCoreInstance core;
    private Features fullFeatures;

    protected MetadataBackend backend;

    protected boolean deleteSupport = true;
    protected boolean findTagsSupport = true;
    protected boolean orFilterSupport = true;
    protected FeatureSet additionalFeatures = FeatureSet.empty();

    protected abstract MetadataModule setupModule() throws Exception;

    protected void setupConditions() { }

    @Before
    public final void abstractSetup() throws Exception {
        final HeroicConfig.Builder fragment = HeroicConfig
            .builder()
            .metadata(MetadataManagerModule.builder()
                .backends(ImmutableList.of(setupModule())));

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

        final List<AsyncFuture<Void>> writesIndexResource = new ArrayList<>();
        writesIndexResource.add(writeSeries(backend, s1, range));
        writesIndexResource.add(writeSeries(backend, s2, range));
        writesIndexResource.add(writeSeries(backend, s3, range));
        async.collectAndDiscard(writesIndexResource).get();

        setupConditions();

        fullFeatures = Features.DEFAULT.applySet(additionalFeatures);
    }

    @After
    public final void abstractTeardown() throws Exception {
        core.shutdown().get();
    }

    @Test
    public void findSeriesComplexTest() throws Exception {
        final FindSeries.Request f =
            new FindSeries.Request(
                and(matchKey("s2"), startsWith("role", "ba")),
                range,
                OptionalLimit.empty(),
                Features.DEFAULT
            );

        assertEquals(ImmutableSet.of(s2), backend.findSeries(f).get().getSeries());
    }

    @Test
    public void findSeriesComplexTestIndexResource() throws Exception {
        final FindSeries.Request f =
            new FindSeries.Request(
                and(matchKey("s2"), startsWith("role", "ba")),
                range,
                OptionalLimit.empty(),
                Features.DEFAULT
            );

        assertEquals(ImmutableSet.of(s2), backend.findSeries(f).get().getSeries());
    }

    @Test
    public void findSeriesComplexWithFeaturesTest() throws Exception {
        assumeFalse(additionalFeatures.isEmpty());

        final FindSeries.Request f =
            new FindSeries.Request(
                and(matchKey("s2"), startsWith("role", "ba")),
                range,
                OptionalLimit.empty(),
                fullFeatures
            );

        assertEquals(ImmutableSet.of(s2), backend.findSeries(f).get().getSeries());
    }

    @Test
    public void findSeriesTest() throws Exception {
        final FindSeries.Request f = new FindSeries.Request(
            TrueFilter.get(), range, OptionalLimit.empty(), Features.DEFAULT);

        final FindSeries result = backend.findSeries(f).get();

        assertEquals(ImmutableSet.of(s1, s2, s3), result.getSeries());
    }

    @Test
    public void findSeriesWithFeaturesTest() throws Exception {
        assumeFalse(additionalFeatures.isEmpty());

        final FindSeries.Request f = new FindSeries.Request(
            TrueFilter.get(), range, OptionalLimit.empty(), fullFeatures);

        final FindSeries result = backend.findSeries(f).get();

        assertEquals(ImmutableSet.of(s1, s2, s3), result.getSeries());
    }

    @Test
    public void findSeriesLimitedTest() throws Exception {
        FindSeries.Request req1 =
            new FindSeries.Request(TrueFilter.get(), range, OptionalLimit.of(1L), Features.DEFAULT);
        final FindSeries r1 = backend
            .findSeries(req1)
            .get();

        assertTrue("Result should be limited", r1.getLimited());
        assertEquals("Result size should be same as limit", 1, r1.getSeries().size());

        FindSeries.Request req2 =
            new FindSeries.Request(TrueFilter.get(), range, OptionalLimit.of(3L), Features.DEFAULT);
        final FindSeries r2 = backend
            .findSeries(req2)
            .get();

        assertFalse("Result should not be limited", r2.getLimited());
        assertEquals("Result size should be all entries", 3, r2.getSeries().size());
    }

    @Test
    public void findSeriesLimitedWithFeaturesTest() throws Exception {
        assumeFalse(additionalFeatures.isEmpty());

        FindSeries.Request req1 =
            new FindSeries.Request(TrueFilter.get(), range, OptionalLimit.of(1L), fullFeatures);
        final FindSeries r1 = backend
            .findSeries(req1)
            .get();

        assertTrue("Result should be limited", r1.getLimited());
        assertEquals("Result size should be same as limit", 1, r1.getSeries().size());

        FindSeries.Request req2 =
            new FindSeries.Request(TrueFilter.get(), range, OptionalLimit.of(3L), fullFeatures);
        final FindSeries r2 = backend
            .findSeries(req2)
            .get();

        assertFalse("Result should not be limited", r2.getLimited());
        assertEquals("Result size should be all entries", 3, r2.getSeries().size());
    }

    @Test
    public void findTags() throws Exception {
        assumeTrue(findTagsSupport);

        final FindTags.Request request =
            new FindTags.Request(TrueFilter.get(), range, OptionalLimit.empty());

        final FindTags result = backend.findTags(request).get();

        assertEquals(ImmutableMap.of("role", ImmutableSet.of("bar", "foo", "baz")),
            result.getTags());
    }

    @Test
    public void findKeys() throws Exception {
        final FindKeys.Request request =
            new FindKeys.Request(TrueFilter.get(), range, OptionalLimit.empty());

        final FindKeys result = backend.findKeys(request).get();

        assertEquals(ImmutableSet.of(s1.getKey(), s2.getKey(), s3.getKey()), result.getKeys());
    }

    @Test
    public void countSeriesTest() throws Exception {
        final CountSeries.Request f =
            new CountSeries.Request(not(matchKey(s2.getKey())), range, OptionalLimit.empty());

        final CountSeries result = backend.countSeries(f).get();
        assertEquals(2L, result.getCount());
    }

    @Test
    public void findSeriesIdsTest() throws Exception {
        final FindSeries.Request f = new FindSeries.Request(
            not(matchKey(s2.getKey())),
            range,
            OptionalLimit.empty(),
            Features.DEFAULT
        );

        final FindSeriesIds result = backend.findSeriesIds(f).get();
        assertEquals(ImmutableSet.of(s1.hash(), s3.hash()), result.getIds());
    }

    @Test
    public void findSeriesIdsWithFeaturesTest() throws Exception {
        assumeFalse(additionalFeatures.isEmpty());

        final FindSeries.Request f = new FindSeries.Request(
            not(matchKey(s2.getKey())),
            range,
            OptionalLimit.empty(),
            fullFeatures
        );

        final FindSeriesIds result = backend.findSeriesIds(f).get();
        assertEquals(ImmutableSet.of(s1.hash(), s3.hash()), result.getIds());
    }

    @Test
    public void deleteSeriesTest() throws Exception {
        assumeTrue(deleteSupport);

        {
            final DeleteSeries.Request request =
                new DeleteSeries.Request(not(matchKey(s2.getKey())), range, OptionalLimit.empty());

            backend.deleteSeries(request).get();
        }

        /* deletes are eventually consistent, wait until they are no longer present
         * but only for a limited period of time */
        retrySome(() -> {
            final FindSeries.Request f = new FindSeries.Request(
                TrueFilter.get(), range, OptionalLimit.empty(), Features.DEFAULT);

            final FindSeries result = backend.findSeries(f).get();

            assertEquals(ImmutableSet.of(s2), result.getSeries());
        });
    }

    @Test
    public void deleteSeriesWithFeaturesTest() throws Exception {
        assumeTrue(deleteSupport);
        assumeFalse(additionalFeatures.isEmpty());

        {
            final DeleteSeries.Request request =
                new DeleteSeries.Request(not(matchKey(s2.getKey())), range, OptionalLimit.empty());

            backend.deleteSeries(request).get();
        }

        /* deletes are eventually consistent, wait until they are no longer present
         * but only for a limited period of time */
        retrySome(() -> {
            final FindSeries.Request f = new FindSeries.Request(
                TrueFilter.get(), range, OptionalLimit.empty(), fullFeatures);

            final FindSeries result = backend.findSeries(f).get();

            assertEquals(ImmutableSet.of(s2), result.getSeries());
        });
    }

    @Test
    public void filterTest() throws Exception {
        assertEquals(ImmutableSet.of(s1.hash(), s2.hash(), s3.hash()), findIds(TrueFilter.get()));
        assertEquals(ImmutableSet.of(), findIds(FalseFilter.get()));

        assertEquals(ImmutableSet.of(s2.hash()), findIds(matchKey(s2.getKey())));

        assertEquals(ImmutableSet.of(s1.hash(), s3.hash()), findIds(not(matchKey(s2.getKey()))));

        assertEquals(ImmutableSet.of(s1.hash()), findIds(matchTag("role", "foo")));

        if (orFilterSupport) {
            assertEquals(ImmutableSet.of(s1.hash(), s2.hash()),
                findIds(or(matchKey(s1.getKey()), matchKey(s2.getKey()))));
        }

        assertEquals(ImmutableSet.of(s1.hash()),
            findIds(and(matchKey(s1.getKey()), matchTag("role", "foo"))));

        assertEquals(ImmutableSet.of(s1.hash(), s2.hash(), s3.hash()), findIds(hasTag("role")));
    }

    /**
     * Retry action for a given period of time.
     *
     * @param action Action to retry if failing
     */
    private void retrySome(final ThrowingRunnable action) throws Exception {
        AssertionError error = null;

        for (int i = 0; i < 20; i++) {
            try {
                action.run();
            } catch (final AssertionError e) {
                if (error != null) {
                    e.addSuppressed(error);
                }

                error = e;
                Thread.sleep(100L);
                continue;
            }

            return;
        }

        throw error;
    }

    private Set<String> findIds(final Filter filter) throws Exception {
        return backend
            .findSeriesIds(
                new FindSeries.Request(filter, range, OptionalLimit.empty(), Features.DEFAULT))
            .get()
            .getIds();
    }

    private AsyncFuture<Void> writeSeries(
        final MetadataBackend metadata, final Series s, final DateRange range
    ) {
        final FindSeries.Request f = new FindSeries.Request(
            MatchKeyFilter.create(s.getKey()),
            range,
            OptionalLimit.empty(),
            Features.DEFAULT
        );

        return metadata.write(new WriteMetadata.Request(s, range)).lazyTransform(ignore -> async
                .retryUntilResolved(() -> metadata.findSeries(f).directTransform(result -> {
                    if (!result.getSeries().contains(s)) {
                        throw new RuntimeException("Expected to find the written series");
                    }

                    return null;
                }), RetryPolicy.timed(10000, RetryPolicy.exponential(100, 200)))
                .directTransform(r -> null));
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }
}
