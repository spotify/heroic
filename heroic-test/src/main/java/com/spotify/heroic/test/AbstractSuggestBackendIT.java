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

import static com.spotify.heroic.filter.Filter.matchKey;
import static org.junit.Assert.assertEquals;

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
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MatchKeyFilter;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.KeySuggest.Suggestion;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.NumSuggestionsLimit;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManagerModule;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagSuggest.Request;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import com.spotify.heroic.suggest.WriteSuggest;
import com.spotify.heroic.test.TimestampPrepender.EntityType;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.RetryPolicy;
import eu.toolchain.async.RetryResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.jetbrains.annotations.NotNull;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public abstract class AbstractSuggestBackendIT {

    // The requests will either not specify a limit or specify one of fifteen.
    public static final int REQ_SUGGESTION_ENTITY_LIMIT = 15;
    public static final String STARTS_WITH_RO = "ro"; // e.g. role
    public static final int EFFECTIVELY_NO_LIMIT = 100_000;
    public static final String BAR = "bar";
    public static final String BAZ = "baz";
    public static final String FOO = "foo";
    public static final String AA = "aa";
    public static final String ROLE = "role";
    public static final String AA_1 = "aa1";
    public static final String AA_2 = "aa2";
    public static final String BB_3 = "bb3";
    // MetaData and Suggest have no concept of datetime ranges so just set
    // the same for all.
    protected static final DateRange UNIVERSAL_RANGE = new DateRange(0L, 0L);
    private static final int SMALL_SERIES_SIZE = 3;
    private static final int LARGE_NUM_ENTITIES = 20;
    private static final int VERY_LARGE_NUM_ENTITIES = 500;

    // longest that we will wait for a write to ES to complete
    public static final int ES_WRITE_MAX_ELAPSED_MS = 20_000;

    // We check to see if the writes are done every 200ms
    public static final int ES_WRITE_CHECK_DONE_WAIT_MS = 200;
    public static final String NO_SUGGESTIONS = "No tag suggestion available for the given "
            + "series";

    protected final String testName = "heroic-it-" + UUID.randomUUID().toString();
    protected AsyncFramework async;
    protected SuggestBackend backend;
    private HeroicCoreInstance core;
    private static final int EXPECTED_TAG_VALUE_COUNT = 20;
    private static final int EXPECTED_KEY_COUNT = 1;

    private static final int EXPECTED_SMALL_SERIES_COUNT = 1; // foo, bar, baz

    protected static List<Pair<Series, DateRange>> createSmallSerieses(long timestamp,
                                                                       EntityType et) {

        var p = new TimestampPrepender(et, timestamp);

        // Create 3 series of the form e.g.
        // { key: aa1, tags: { role, foo } }
        return new ArrayList<>() {
            {
                add(createSeriesPair(AA_1, FOO, p));
                add(createSeriesPair(AA_2, BAR, p));
                add(createSeriesPair(BB_3, BAZ, p));
            }

            @NotNull
            private ImmutablePair<Series, DateRange> createSeriesPair(String key, String tagValue,
                                                                      TimestampPrepender p) {
                return new ImmutablePair<>(Series.of(
                        p.prepend(key, EntityType.KEY),
                        ImmutableMap.of(p.prepend(ROLE, EntityType.TAG),
                                p.prepend(tagValue, EntityType.TAG_VALUE))), UNIVERSAL_RANGE);
            }
        };
    }

    private static TagKeyCount.Request createTagCountRequest(long timestamp) {
        return new TagKeyCount.Request(TrueFilter.get(),
                new DateRange(timestamp, timestamp), OptionalLimit.empty(), OptionalLimit.empty());
    }

    @NotNull
    private static TagValuesSuggest.Request buildTagValuesRequest(
            OptionalLimit numSuggestionsLimit) {
        return new TagValuesSuggest.Request(TrueFilter.get(),
                UNIVERSAL_RANGE, numSuggestionsLimit,
                OptionalLimit.of(EFFECTIVELY_NO_LIMIT), ImmutableList.of());
    }

    @NotNull
    private static TagValuesSuggest.Request buildTagValuesRequest(
            Filter filter,
            OptionalLimit numSuggestionsLimit) {
        return new TagValuesSuggest.Request(filter,
                UNIVERSAL_RANGE, numSuggestionsLimit,
                OptionalLimit.of(EFFECTIVELY_NO_LIMIT), ImmutableList.of());
    }

    private static TagValueSuggest.Request buildTagValueSuggestReq(
            String tagValue, long timestamp, OptionalLimit numSuggestionsLimit) {

        return new TagValueSuggest.Request(TrueFilter.get(),
                UNIVERSAL_RANGE, numSuggestionsLimit,
                Optional.of(TimestampPrepender.prepend(timestamp, tagValue)));
    }

    @NotNull
    private static Request buildTagSuggestRequest(String tagValue, long timestamp) {
        return new Request(
                TrueFilter.get(), UNIVERSAL_RANGE, OptionalLimit.empty(),
                MatchOptions.builder().build(),
                Optional.of(TimestampPrepender.prepend(timestamp, tagValue)),
                Optional.empty());
    }

    @NotNull
    private static Request buildTagSuggestRequest(
            String tagValue, long timestamp, int numSuggestionsLimit) {
        return new Request(TrueFilter.get(), UNIVERSAL_RANGE,
                OptionalLimit.of(numSuggestionsLimit),
                MatchOptions.builder().build(),
                Optional.of(TimestampPrepender.prepend(timestamp, tagValue)), Optional.empty());
    }

    private static KeySuggest.Request keySuggestStartsWithReq(String startsWith, long timestamp) {
        return keySuggestStartsWithReq(startsWith, timestamp, OptionalLimit.empty());
    }

    private static KeySuggest.Request keySuggestStartsWithReq(String startsWith, long timestamp,
                                                              OptionalLimit numSuggestionsLimit) {

        return new KeySuggest.Request(TrueFilter.get(), UNIVERSAL_RANGE, numSuggestionsLimit,
                MatchOptions.builder().build(),
                Optional.of(TimestampPrepender.prepend(timestamp, startsWith)));
    }

    private static long getUniqueTimestamp() {
        final long t = Instant.now().toEpochMilli() + (long) Math.random();
        return t;
    }

    private static List<Pair<Series, DateRange>> createTestSeriesData(int numKeys,
                                                                      int tagsAndTagValuesPerKey,
                                                                      long timestamp,
                                                                      Set<EntityType> et) {
        var p = new TimestampPrepender(et, timestamp);
        return createSeriesTestDataImpl(numKeys, tagsAndTagValuesPerKey, p);

    }

    private static List<Pair<Series, DateRange>> createTestSeriesData(int numKeys,
                                                                      int tagsAndTagValuesPerKey,
                                                                      long timestamp,
                                                                      EntityType et) {
        var p = new TimestampPrepender(et, timestamp);
        return createSeriesTestDataImpl(numKeys, tagsAndTagValuesPerKey, p);
    }

    @NotNull
    private static ArrayList<Pair<Series, DateRange>> createSeriesTestDataImpl(
            int numKeys,
            int tagsAndTagValuesPerKey,
            TimestampPrepender p) {
        var series = new ArrayList<Pair<Series, DateRange>>(numKeys);

        for (int i = 0; i < numKeys; i++) {
            final var key = p.prepend(String.format(AA + "-%d", i + 1), EntityType.KEY);
            for (int j = 0; j < tagsAndTagValuesPerKey; j++) {

                final var tags =
                        ImmutableMap.of(
                                p.prepend(ROLE, EntityType.TAG),
                                p.prepend(FOO + "-" + (j + 1), EntityType.TAG_VALUE));

                series.add(new ImmutablePair<>(Series.of(key, tags), UNIVERSAL_RANGE));
            }
        }

        return series;
    }

    protected abstract SuggestModule setupModule() throws Exception;

    @Before
    public final void abstractSetup() throws Exception {
        final HeroicConfig.Builder fragment = HeroicConfig.builder()
                .suggest(SuggestManagerModule.builder().backends(ImmutableList.of(setupModule())));

        core = HeroicCore.builder().setupService(false).setupShellServer(false)
                .configFragment(fragment).build()
                .newInstance();

        core.start().get();

        async = core.inject(LoadingComponent::async);

        backend = core.inject(
                c -> c.suggestManager().groupSet().inspectAll().stream()
                        .map(GroupMember::getMember).findFirst()).orElseThrow(
                () -> new IllegalStateException("Failed to find backend"));
    }

    @After
    public final void abstractTeardown() throws Exception {
        core.shutdown().get();
    }

    @Test
    public void tagValuesSuggestSmall() throws Exception {
        // Check a single suggestion with values
        final long timestamp = getUniqueTimestamp();

        writeSeries(backend,
            createSmallSerieses(timestamp, EntityType.TAG), EXPECTED_SMALL_SERIES_COUNT);

        var result = getTagValuesSuggest(
                buildTagValuesRequest(OptionalLimit.empty()));
        var suggestion = result.getSuggestions().get(0);

        var expected = new TreeSet<String>(Arrays.asList(BAR, BAZ, FOO));

        assertEquals(new TagValuesSuggest.Suggestion(TimestampPrepender.prepend(timestamp,
                ROLE), expected, false), suggestion);
    }

    @Test
    public void tagValuesTruncatedSuggest() throws Exception {
        tagValuesTruncatedSuggestImpl(0);
    }

    /**
     * This is to investigate the different behaviour we're seeing between running
     * the IT's locally and in CI.
     */
    @Test
    public void tagValuesTruncatedSuggestMany() throws Exception {
        for (int i = 0; i < 15; i++) {
            tagValuesTruncatedSuggestImpl(i + 1);
        }
    }

    private void tagValuesTruncatedSuggestImpl(int count) throws Exception {
        // Check that a number of tag values larger than the supplied limit is
        // correctly truncated.
        final long timestamp = getUniqueTimestamp();
        var entitiesToTimestamp = Set.of(EntityType.KEY, EntityType.TAG);
        var p = new TimestampPrepender(entitiesToTimestamp, timestamp);

        var seriesData =
                createTestSeriesData(1, LARGE_NUM_ENTITIES, timestamp, entitiesToTimestamp);
        writeSeries(backend, seriesData, EXPECTED_TAG_VALUE_COUNT);

        final var key = p.prepend(AA + "-1", EntityType.KEY);
        var result =
                getTagValuesSuggest(
                        buildTagValuesRequest(MatchKeyFilter.create(key),
                                OptionalLimit.of(REQ_SUGGESTION_ENTITY_LIMIT)));

        final var suggestions = result.getSuggestions();
        assertEquals(1, suggestions.size());
        Assert.assertEquals("Iteration " + count + " not equal", REQ_SUGGESTION_ENTITY_LIMIT,
                suggestions.get(0).getValues().size());
    }

    @Test
    public void tagKeyCount() throws Exception {
        final long timestamp = getUniqueTimestamp();

        var smallTestSeries = createSmallSerieses(timestamp, EntityType.TAG);
        writeSeries(backend, smallTestSeries, EXPECTED_SMALL_SERIES_COUNT);

        final TagKeyCount result = getTagKeyCount(createTagCountRequest(timestamp));
        final TagKeyCount.Suggestion s = result.getSuggestions().get(0);

        assertEquals(TimestampPrepender.prepend(timestamp, ROLE), s.getKey());
        assertEquals(3, s.getCount());
    }

    /**
     * Check we get the expected tag and 3 results
     */
    @Test
    public void tagSuggestSmall() throws Exception {

        final long timestamp = getUniqueTimestamp();
        var smallTestSeries =
                createSmallSerieses(timestamp, EntityType.TAG);
        writeSeries(backend, smallTestSeries, EXPECTED_SMALL_SERIES_COUNT); // adds 3 tags

        var result = getTagSuggest(
                buildTagSuggestRequest(STARTS_WITH_RO, timestamp));

        assertEquals(SMALL_SERIES_SIZE, result.size());
        assertEquals(TimestampPrepender.prepend(timestamp, ROLE),
                result.stream().findFirst().get().getKey());
    }

    /**
     * Check that a request limit is respected and one without gets the whole lot.
     */
    @Test
    public void tagSuggestLimit() throws Exception {

        long timestamp = getUniqueTimestamp();

        // add LARGE_NUM_ENTITIES tags. Total is now 23
        var largeNumTagsSeries =
                createTestSeriesData(1, LARGE_NUM_ENTITIES, timestamp, EntityType.TAG);
        writeSeries(backend, largeNumTagsSeries, EXPECTED_TAG_VALUE_COUNT);

        var result =
                getTagSuggest(buildTagSuggestRequest(STARTS_WITH_RO, timestamp,
                        REQ_SUGGESTION_ENTITY_LIMIT));

        assertEquals(REQ_SUGGESTION_ENTITY_LIMIT, result.size());

        // Check that the request without a limit returns the whole lot. Note that
        // the maximum number of tags for a key is LARGE_NUM_ENTITIES - see
        // createTestSeriesData.
        result = getTagSuggest(buildTagSuggestRequest(STARTS_WITH_RO, timestamp));
        assertEquals(LARGE_NUM_ENTITIES, result.size());
    }

    /**
     * Check that a hard ceiling of NumSuggestionsLimit.LIMIT_CEILING is respected
     *
     * @throws Exception
     */
    @Test
    public void tagSuggestCeiling() throws Exception {

        long timestamp = getUniqueTimestamp();
        var veryLargeNumTagsSeries =
                createTestSeriesData(1, VERY_LARGE_NUM_ENTITIES, timestamp, EntityType.TAG);
        writeSeries(backend, veryLargeNumTagsSeries, NumSuggestionsLimit.DEFAULT_LIMIT);

        var reqStartsWithRo = buildTagSuggestRequest(STARTS_WITH_RO, timestamp,
                AbstractSuggestBackendIT.EFFECTIVELY_NO_LIMIT);
        var result = getTagSuggest(reqStartsWithRo);
        assertEquals(NumSuggestionsLimit.LIMIT_CEILING, result.size());
    }

    @Test
    public void tagValueSuggestSmall() throws Exception {
        final long timestamp = getUniqueTimestamp();

        writeSeries(backend,
            createSmallSerieses(timestamp, EntityType.TAG), EXPECTED_SMALL_SERIES_COUNT);

        var result = getTagValueSuggest(
                buildTagValueSuggestReq(ROLE, timestamp, OptionalLimit.empty()));

        var expected = new TreeSet<String>(Arrays.asList(BAR, BAZ, FOO));
        assertEquals(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(result.getValues()));
    }

    @Test
    public void tagValueSuggestLimited() throws Exception {
        final long timestamp = getUniqueTimestamp();

        var largeNumTagsSeries =
                createTestSeriesData(1, LARGE_NUM_ENTITIES, timestamp, EntityType.TAG);

        writeSeries(backend, largeNumTagsSeries, EXPECTED_TAG_VALUE_COUNT);

        var result = getTagValueSuggest(
                buildTagValueSuggestReq(ROLE, timestamp,
                        OptionalLimit.of(REQ_SUGGESTION_ENTITY_LIMIT)));

        assertEquals(REQ_SUGGESTION_ENTITY_LIMIT, result.getValues().size());
    }

    @Test
    public void keySuggest() throws Exception {
        var et = EntityType.KEY;
        {
            final long timestamp = getUniqueTimestamp();
            var smallTestSeries = createSmallSerieses(timestamp, et);

            writeSeries(backend, smallTestSeries, EXPECTED_KEY_COUNT);

            var result = getKeySuggest(keySuggestStartsWithReq(AA, timestamp));
            assertEquals(ImmutableSet.of(TimestampPrepender.prepend(timestamp, AA_1),
                    TimestampPrepender.prepend(timestamp,
                            AA_2)),
                    result);
        }

        {
            final long timestamp = getUniqueTimestamp();

            var largeNumKeysSeries =
                    createTestSeriesData(LARGE_NUM_ENTITIES, 1, timestamp, EntityType.KEY);

            writeSeries(backend, largeNumKeysSeries, EXPECTED_KEY_COUNT);

            var result =
                    getKeySuggest(
                            keySuggestStartsWithReq(
                                    AA, timestamp, OptionalLimit.of(REQ_SUGGESTION_ENTITY_LIMIT)));
            assertEquals(REQ_SUGGESTION_ENTITY_LIMIT, result.size());
        }
    }

    @Test
    public void tagValueSuggestNoIdx() throws Exception {
        final TagValueSuggest result = getTagValueSuggest(
                buildTagValueSuggestReq(ROLE, 0L, OptionalLimit.empty()));

        assertEquals(Collections.emptyList(), result.getValues());
    }

    @Test
    public void tagValuesSuggestNoIdx() throws Exception {
        final TagValuesSuggest result = getTagValuesSuggest(
                buildTagValuesRequest(OptionalLimit.empty()));

        assertEquals(Collections.emptyList(), result.getSuggestions());
    }

    @Test
    public void tagKeyCountNoIdx() throws Exception {
        final long timestamp = getUniqueTimestamp();

        var tagKeyCountReq = createTagCountRequest(timestamp);
        final TagKeyCount result = getTagKeyCount(tagKeyCountReq);

        assertEquals(Collections.emptyList(), result.getSuggestions());
    }

    @Test
    public void tagSuggestNoIdx() throws Exception {
        final Set<Pair<String, String>> result =
                getTagSuggest(buildTagSuggestRequest("ba", getUniqueTimestamp()));

        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void keySuggestNoIdx() throws Exception {
        final Set<String> result =
                getKeySuggest(keySuggestStartsWithReq(AA, getUniqueTimestamp()));

        assertEquals(Collections.emptySet(), result);
    }

    private AsyncFuture<RetryResult<Collection<Integer>>> writeSeries(
            final SuggestBackend suggest, final Series s, final DateRange range,
             int expectedTagValueCount) {

        return suggest
                .write(new WriteSuggest.Request(s, range))
                .lazyTransform(
                        r -> async
                                // we wait up to ES_WRITE_MAX_ELAPSED_MS millis for the write to
                                // succeed. We test to see if it's done every
                                // ES_WRITE_RETRY_WAIT_MS millis.
                                .retryUntilResolved(
                                        () ->
                                                checks(s, range, expectedTagValueCount),
                                        RetryPolicy.timed(
                                                ES_WRITE_MAX_ELAPSED_MS,
                                                RetryPolicy.linear(ES_WRITE_CHECK_DONE_WAIT_MS)
                                        )
                                )
                );
    }

    private AsyncFuture<Collection<Integer>> checks(final Series s, DateRange range,
                                                    int expectedTagValueCount) {
        final List<AsyncFuture<Integer>> checks = new ArrayList<>();

        checks.add(
                backend.tagSuggest(
                        new TagSuggest.Request(
                                matchKey(
                                        s.getKey()
                                ),
                                range,
                                OptionalLimit.empty(),
                                MatchOptions.builder().build(),
                                Optional.empty(),
                                Optional.empty()))
                        .directTransform(
                                result -> {
                                    var suggestions = result.getSuggestions();
                                    int count = suggestions == null ? 0 : suggestions.size();
                                    if (suggestions.size() != expectedTagValueCount) {
                                        throw new IllegalStateException(
                                                suggestions.size() + "/" + expectedTagValueCount +
                                                        " tag suggestions returned. Will wait for" +
                                                         " the rest.");
                                    }

                                    return suggestions.size();
                                }));

        checks.add(
                backend.keySuggest(
                        new KeySuggest.Request(
                                matchKey(
                                        s.getKey()
                                ),
                                range,
                                OptionalLimit.empty(),
                                MatchOptions.builder().build(),
                                Optional.empty()))
                        .directTransform(
                                result -> {
                                    if (result.getSuggestions().isEmpty()) {
                                        throw new IllegalStateException(
                                                "No key suggestion available for the given series");
                                    }

                                    return result.getSuggestions().size();
                                }));

        return async.collect(checks);
    }

    private void writeSeries(final SuggestBackend backend,
                             final List<Pair<Series, DateRange>> data,
                             int expectedTagValueCount)
            throws Exception {

        final List<AsyncFuture<RetryResult<Collection<Integer>>>> writes = new ArrayList<>();
        for (Pair<Series, DateRange> p : data) {
            writes.add(writeSeries(backend, p.getKey(), p.getValue(), expectedTagValueCount));
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
        return backend.tagSuggest(req).get().getSuggestions().stream()
                .map(s -> Pair.of(s.getKey(), s.getValue())).collect(Collectors.toSet());
    }

    private Set<String> getKeySuggest(final KeySuggest.Request req)
            throws ExecutionException, InterruptedException {
        return backend.keySuggest(req).get().getSuggestions().stream()
                .map(Suggestion::getKey).collect(Collectors.toSet());
    }
}
