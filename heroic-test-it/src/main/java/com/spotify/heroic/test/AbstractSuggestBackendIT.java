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
import com.google.common.collect.ImmutableSortedSet;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.GroupMember;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.LoadingComponent;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManagerModule;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import com.spotify.heroic.suggest.WriteSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.RetryPolicy;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.spotify.heroic.filter.Filter.matchKey;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractSuggestBackendIT {
    private final Series s1 = Series.of("aa1", ImmutableMap.of("role", "foo"));
    private final Series s2 = Series.of("aa2", ImmutableMap.of("role", "bar"));
    private final Series s3 = Series.of("bb3", ImmutableMap.of("role", "baz"));

    protected final DateRange range = new DateRange(0L, 0L);

    private final List<Pair<Series, DateRange>> testSeries =
        new ArrayList<Pair<Series, DateRange>>() {
            {
                add(new ImmutablePair<>(s1, range));
                add(new ImmutablePair<>(s2, range));
                add(new ImmutablePair<>(s3, range));
            }
        };

    private final TagValuesSuggest.Request tagValuesSuggestReq =
        new TagValuesSuggest.Request(TrueFilter.get(), range, OptionalLimit.empty(),
            OptionalLimit.empty(), ImmutableList.of());

    private final TagValueSuggest.Request tagValueSuggestReq =
        new TagValueSuggest.Request(TrueFilter.get(), range, OptionalLimit.empty(),
            Optional.of("role"));

    private final TagKeyCount.Request tagKeyCountReq =
        new TagKeyCount.Request(TrueFilter.get(), range, OptionalLimit.empty(),
            OptionalLimit.empty());

    private final TagSuggest.Request tagSuggestReq =
        new TagSuggest.Request(TrueFilter.get(), range, OptionalLimit.empty(),
            MatchOptions.builder().build(), Optional.empty(), Optional.of("ba"));

    private final KeySuggest.Request keySuggestReq =
        new KeySuggest.Request(TrueFilter.get(), range, OptionalLimit.empty(),
            MatchOptions.builder().build(), Optional.of("aa"));

    private HeroicCoreInstance core;

    protected AsyncFramework async;
    protected SuggestBackend backend;

    protected abstract SuggestModule setupModule() throws Exception;

    @Before
    public final void abstractSetup() throws Exception {
        final HeroicConfig.Builder fragment = HeroicConfig
            .builder()
            .suggest(SuggestManagerModule.builder().backends(ImmutableList.of(setupModule())));

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
                .suggestManager()
                .groupSet()
                .inspectAll()
                .stream()
                .map(GroupMember::getMember)
                .findFirst())
            .orElseThrow(() -> new IllegalStateException("Failed to find backend"));
    }

    @After
    public final void abstractTeardown() throws Exception {
        core.shutdown().get();
    }

    @Test
    public void tagValuesSuggest() throws Exception {
        writeSeries(backend, testSeries);

        final TagValuesSuggest result = getTagValuesSuggest(tagValuesSuggestReq);
        final TagValuesSuggest.Suggestion s = result.getSuggestions().get(0);

        assertEquals(
            new TagValuesSuggest.Suggestion("role", ImmutableSortedSet.of("bar", "baz", "foo"),
                false), s);
    }

    @Test
    public void tagValueSuggest() throws Exception {
        writeSeries(backend, testSeries);

        final TagValueSuggest result = getTagValueSuggest(tagValueSuggestReq);

        assertEquals(ImmutableSet.of("bar", "baz", "foo"), ImmutableSet.copyOf(result.getValues()));
    }

    @Test
    public void tagKeyCount() throws Exception {
        writeSeries(backend, testSeries);

        final TagKeyCount result = getTagKeyCount(tagKeyCountReq);
        final TagKeyCount.Suggestion s = result.getSuggestions().get(0);

        assertEquals("role", s.getKey());
        assertEquals(3, s.getCount());
    }

    @Test
    public void tagSuggest() throws Exception {
        writeSeries(backend, testSeries);

        final Set<Pair<String, String>> result = getTagSuggest(tagSuggestReq);

        assertEquals(ImmutableSet.of(Pair.of("role", "bar"), Pair.of("role", "baz")), result);
    }

    @Test
    public void keySuggest() throws Exception {
        writeSeries(backend, testSeries);

        final Set<String> result = getKeySuggest(keySuggestReq);

        assertEquals(ImmutableSet.of(s1.getKey(), s2.getKey()), result);
    }

    @Test
    public void tagValueSuggestNoIdx() throws Exception {
        final TagValueSuggest result = getTagValueSuggest(tagValueSuggestReq);

        assertEquals(Collections.emptyList(), result.getValues());
    }

    @Test
    public void tagValuesSuggestNoIdx() throws Exception {
        final TagValuesSuggest result = getTagValuesSuggest(tagValuesSuggestReq);

        assertEquals(Collections.emptyList(), result.getSuggestions());
    }

    @Test
    public void tagKeyCountNoIdx() throws Exception {
        final TagKeyCount result = getTagKeyCount(tagKeyCountReq);

        assertEquals(Collections.emptyList(), result.getSuggestions());
    }

    @Test
    public void tagSuggestNoIdx() throws Exception {
        final Set<Pair<String, String>> result = getTagSuggest(tagSuggestReq);

        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void keySuggestNoIdx() throws Exception {
        final Set<String> result = getKeySuggest(keySuggestReq);

        assertEquals(Collections.emptySet(), result);
    }

    private AsyncFuture<Void> writeSeries(
        final SuggestBackend suggest, final Series s, final DateRange range
    ) throws Exception {
        return suggest
            .write(new WriteSuggest.Request(s, range))
            .lazyTransform(r -> async.retryUntilResolved(() -> checks(s),
                RetryPolicy.timed(10000, RetryPolicy.exponential(100, 200))))
            .directTransform(retry -> null);
    }

    private AsyncFuture<Void> checks(final Series s) {
        final List<AsyncFuture<Void>> checks = new ArrayList<>();

        checks.add(backend
            .tagSuggest(new TagSuggest.Request(matchKey(s.getKey()), range, OptionalLimit.empty(),
                MatchOptions.builder().build(), Optional.empty(), Optional.empty()))
            .directTransform(result -> {
                if (result.getSuggestions().isEmpty()) {
                    throw new IllegalStateException("No suggestion available for the given series");
                }

                return null;
            }));

        checks.add(backend
            .keySuggest(new KeySuggest.Request(matchKey(s.getKey()), range, OptionalLimit.empty(),
                MatchOptions.builder().build(), Optional.empty()))
            .directTransform(result -> {
                if (result.getSuggestions().isEmpty()) {
                    throw new IllegalStateException("No suggestion available for the given series");
                }

                return null;
            }));

        return async.collectAndDiscard(checks);
    }

    private void writeSeries(final SuggestBackend backend, final List<Pair<Series, DateRange>> data)
        throws Exception {

        final List<AsyncFuture<Void>> writes = new ArrayList<>();
        for (Pair<Series, DateRange> p : data) {
            writes.add(writeSeries(backend, p.getKey(), p.getValue()));
        }
        async.collectAndDiscard(writes).get();
    }

    private TagValuesSuggest getTagValuesSuggest(final TagValuesSuggest.Request req)
        throws ExecutionException, InterruptedException {
        return backend.tagValuesSuggest(req).get();
    }

    private TagValueSuggest getTagValueSuggest(final TagValueSuggest.Request req)
        throws ExecutionException, InterruptedException {
        return backend.tagValueSuggest(req).get();
    }

    private TagKeyCount getTagKeyCount(final TagKeyCount.Request req)
        throws ExecutionException, InterruptedException {
        return backend.tagKeyCount(req).get();
    }

    private Set<Pair<String, String>> getTagSuggest(final TagSuggest.Request req)
        throws ExecutionException, InterruptedException {
        return backend
            .tagSuggest(req)
            .get()
            .getSuggestions()
            .stream()
            .map(s -> Pair.of(s.getKey(), s.getValue()))
            .collect(Collectors.toSet());
    }

    private Set<String> getKeySuggest(final KeySuggest.Request req)
        throws ExecutionException, InterruptedException {
        return backend
            .keySuggest(req)
            .get()
            .getSuggestions()
            .stream()
            .map(s -> s.getKey())
            .collect(Collectors.toSet());
    }
}
