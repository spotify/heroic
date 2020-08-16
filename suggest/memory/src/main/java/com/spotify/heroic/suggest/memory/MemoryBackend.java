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

package com.spotify.heroic.suggest.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.NumSuggestionsLimit;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import com.spotify.heroic.suggest.WriteSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import org.jetbrains.annotations.NotNull;

@MemoryScope
public class MemoryBackend implements SuggestBackend, Grouped {
    private static final float SCORE = 1.0f;

    private final SortedMap<String, Set<String>> keys = new TreeMap<>();
    private final SortedMap<String, Set<TagId>> tagKeys = new TreeMap<>();
    private final SortedMap<String, Set<TagId>> tagValues = new TreeMap<>();

    private final HashMap<String, KeyDocument> keyIndex = new HashMap<>();
    private final HashMap<TagId, TagDocument> tagIndex = new HashMap<>();
    private final SortedSet<Series> series = new TreeSet<>();

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Groups groups;
    private final AsyncFramework async;

    private NumSuggestionsLimit numSuggestionsLimit = NumSuggestionsLimit.of();

    @Inject
    public MemoryBackend(final Groups groups, final AsyncFramework async,
        Integer numSuggestionsIntLimit) {
        this.groups = groups;
        this.async = async;
        this.numSuggestionsLimit = NumSuggestionsLimit.of(numSuggestionsIntLimit);
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(TagValuesSuggest.Request request) {

        final var tagsToValues = getTagsToValuesLimited(request.getGroupLimit(), request.getLimit(), request.getFilter());

        final var suggestions = ImmutableList.copyOf(
                tagsToValues.entrySet().stream()
                        .map(e -> new TagValuesSuggest.Suggestion(e.getKey(),
                                ImmutableSortedSet.copyOf(e.getValue()), false))
                        .iterator());

        return async.resolved(new TagValuesSuggest(suggestions, false));
    }

    @Override
    public AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request) {
        final Map<String, Set<String>> counts = new HashMap<>();

        try (final Stream<Series> series = lookupSeries(request.getFilter())) {
            series.forEach(s -> {
                for (final Map.Entry<String, String> e : s.getTags().entrySet()) {
                    Set<String> c = counts.get(e.getKey());

                    if (c == null) {
                        c = new HashSet<>();
                        counts.put(e.getKey(), c);
                    }

                    c.add(e.getValue());
                }
            });
        }

        final List<TagKeyCount.Suggestion> suggestions = ImmutableList.copyOf(request
            .getLimit()
            .limitStream(counts.entrySet().stream())
            .map(e -> new TagKeyCount.Suggestion(e.getKey(), (long) e.getValue().size(),
                Optional.empty()))
            .iterator());

        return async.resolved(new TagKeyCount(suggestions, false));
    }

    @Override
    public AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request) {
        final Optional<Set<String>> keys = request.getKey().map(MemoryBackend::analyze);
        final Optional<Set<String>> values = request.getValue().map(MemoryBackend::analyze);

        try (final Stream<TagDocument> docs = lookupTags(request.getFilter())) {
            final Set<TagId> ids = docs.map(TagDocument::getId).collect(Collectors.toSet());

            keys.ifPresent(parts -> parts.forEach(
                k -> ids.retainAll(tagKeys.getOrDefault(k, ImmutableSet.of()))));

            values.ifPresent(parts -> parts.forEach(
                k -> ids.retainAll(tagValues.getOrDefault(k, ImmutableSet.of()))));

            int limit = numSuggestionsLimit.calculateNewLimit(request.getLimit());

            final List<TagSuggest.Suggestion> suggestions = ImmutableList.copyOf(
                    ImmutableSortedSet.copyOf(ids.stream()
                            .limit(limit)
                            .map(tagIndex::get)
                            .filter(Objects::nonNull)
                            .map(d -> new TagSuggest.Suggestion(
                                    SCORE, d.getId().getKey(), d.getId().getValue()))
                            .iterator()));

            return async.resolved(new TagSuggest(suggestions));
        }
    }

    @Override
    public AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request) {
        final Optional<Set<String>> analyzedKeys = request.getKey().map(MemoryBackend::analyze);

        final Set<String> ids;

        try (final Stream<KeyDocument> docs = lookupKeys(request.getFilter())) {
            ids = docs.map(KeyDocument::getId).collect(Collectors.toSet());

            analyzedKeys.ifPresent(parts -> parts.forEach(
                k -> ids.retainAll(keys.getOrDefault(k, ImmutableSet.of()))));
        }

        final List<KeySuggest.Suggestion> suggestions = ImmutableList.copyOf(request
            .getLimit()
            .limitStream(ids.stream())
            .map(d -> new KeySuggest.Suggestion(SCORE, d))
            .iterator());

        return async.resolved(new KeySuggest(suggestions));
    }


    @Override
    public AsyncFuture<TagValueSuggest> tagValueSuggest(final TagValueSuggest.Request request) {
        try (final Stream<TagDocument> docs = lookupTags(request.getFilter())) {
            final Stream<TagId> ids = docs.map(TagDocument::getId);

            int limit = numSuggestionsLimit.calculateNewLimit(request.getLimit());

            final var tagIdStream = request
                    .getKey()
                    .map(k -> ids.filter(id -> id.getKey().equals(k)))
                    .orElse(ids);

            final List<String> values = tagIdStream
                    .limit(limit)
                    .map(TagId::getValue)
                    .collect(Collectors.toList());

            return async.resolved(new TagValueSuggest(values, false));
        }
    }

    @Override
    public AsyncFuture<WriteSuggest> write(final WriteSuggest.Request request) {
        final Series s = request.getSeries();

        final Lock l = lock.writeLock();

        l.lock();

        try {
            series.add(s);

            keyIndex.put(s.getKey(), new KeyDocument(s.getKey(), s));

            for (final String t : analyze(s.getKey())) {
                putEntry(keys, t, s.getKey());
            }

            for (final Map.Entry<String, String> tag : s.getTags().entrySet()) {
                final TagId id = new TagId(tag.getKey(), tag.getValue());

                tagIndex.put(id, new TagDocument(id, s));

                for (final String t : analyze(tag.getKey())) {
                    putEntry(tagKeys, t, id);
                }

                for (final String t : analyze(tag.getValue())) {
                    putEntry(tagValues, t, id);
                }
            }

            return async.resolved(new WriteSuggest());
        } finally {
            l.unlock();
        }
    }

    private <K, V> void putEntry(
        final SortedMap<K, Set<V>> index, final K key, final V value
    ) {
        Set<V> store = index.get(key);

        if (store == null) {
            store = new HashSet<>();
            index.put(key, store);
        }

        store.add(value);
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public Groups groups() {
        return groups;
    }

    private static final Pattern p = Pattern.compile("([^a-zA-Z0-9]+|(?<=[a-z0-9])(?=[A-Z]))");

    static Set<String> analyze(final String input) {
        if (input.isEmpty()) {
            return ImmutableSet.of();
        }

        final String[] parts = p.split(input);
        final Set<String> output = new HashSet<>();

        for (final String p : parts) {
            final String l = p.toLowerCase();

            if (l.length() == 0) {
                continue;
            }

            output.add(l);
            output.addAll(prefix(l));
        }

        return output;
    }

    private static Collection<String> prefix(final String input) {
        final Set<String> prefixes = new HashSet<>(36);

        for (int i = 1; i < Math.min(input.length(), 20); i++) {
            prefixes.add(input.substring(0, i));
        }

        return prefixes;
    }

    private Stream<KeyDocument> lookupKeys(final Filter filter) {
        final Lock l = lock.readLock();
        l.lock();
        return keyIndex.values().stream()
            .filter(e -> filter.apply(e.getSeries()))
            .onClose(l::unlock);
    }

    private Stream<TagDocument> lookupTags(final Filter filter) {
        final Lock l = lock.readLock();
        l.lock();
        return tagIndex.values().stream()
                .filter(e -> filter.apply(e.getSeries()))
                .onClose(l::unlock);
    }

    private Stream<Series> lookupSeries(final Filter filter) {
        final Lock l = lock.readLock();
        l.lock();
        return series.stream().filter(filter::apply).onClose(l::unlock);
    }

    /**
     * @param groupLimit    maximum number of groups allowed
     * @param limit
     * @param requestFilter defines which series will be returned
     * @return { tag → { val1, val2 }, tag2 → { val2, val9 }, ...}
     */
    @NotNull
    private Map<String, Set<String>> getTagsToValuesLimited(OptionalLimit groupLimit, OptionalLimit requestLimit, Filter requestFilter) {

        final Map<String, Set<String>> allTagsToValuesMap = new HashMap<>();

        final int limit = numSuggestionsLimit.calculateNewLimit(requestLimit);

        try (final Stream<Series> seriesStream = lookupSeries(requestFilter)) {
            return populateLimitedTagsToValuesMap(groupLimit, limit, seriesStream);
        }

    }

    private Map<String, Set<String>> populateLimitedTagsToValuesMap(OptionalLimit groupLimit, int requestLimit, Stream<Series> seriesStream) {

        // PSK TODO find alternative as this is slow since it flushes processor core caches
        // and synchronizes them :/
        AtomicLong totalNumValues = new AtomicLong();
        var allTagsToValuesMap = new HashMap<String, Set<String>>();

        seriesStream.forEach(series -> {
            for (final Map.Entry<String, String> tagValuePair : series.getTags().entrySet()) {
                Set<String> values = allTagsToValuesMap.get(tagValuePair.getKey());

                // If you've not seen this tag before, create a holder for its
                // values
                if (values==null) {
                    values = new HashSet<>();
                    allTagsToValuesMap.put(tagValuePair.getKey(), values);
                }

                final int numValues = values.size();

                if (groupLimit.isGreaterOrEqual(numValues)) {
                    continue;
                }

                if (totalNumValues.incrementAndGet() > requestLimit) {
                    continue;
                }

                // Add the value to the collection of values for this tag
                values.add(tagValuePair.getValue());
            }
        });

        return allTagsToValuesMap;
    }

    public String toString() {
        return "MemoryBackend()";
    }
}
