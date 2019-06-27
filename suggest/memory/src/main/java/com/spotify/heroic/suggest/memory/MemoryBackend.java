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
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.inject.Inject;
import lombok.Data;

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

    @Inject
    public MemoryBackend(final Groups groups, final AsyncFramework async) {
        this.groups = groups;
        this.async = async;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(TagValuesSuggest.Request request) {
        final Map<String, Set<String>> counts = new HashMap<>();

        final OptionalLimit groupLimit = request.getGroupLimit();

        try (final Stream<Series> series = lookupSeries(request.getFilter())) {
            series.forEach(s -> {
                for (final Map.Entry<String, String> e : s.getTags().entrySet()) {
                    Set<String> c = counts.get(e.getKey());

                    if (c == null) {
                        c = new HashSet<>();
                        counts.put(e.getKey(), c);
                    }

                    if (groupLimit.isGreaterOrEqual(c.size())) {
                        continue;
                    }

                    c.add(e.getValue());
                }
            });
        }

        final List<TagValuesSuggest.Suggestion> suggestions = ImmutableList.copyOf(request
            .getLimit()
            .limitStream(counts.entrySet().stream())
            .map(e -> new TagValuesSuggest.Suggestion(e.getKey(),
                ImmutableSortedSet.copyOf(e.getValue()), false))
            .iterator());

        return async.resolved(TagValuesSuggest.of(suggestions, false));
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

        return async.resolved(TagKeyCount.of(suggestions, false));
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

            final List<TagSuggest.Suggestion> suggestions = ImmutableList.copyOf(
                ImmutableSortedSet.copyOf(request
                    .getLimit()
                    .limitStream(ids.stream())
                    .map(tagIndex::get)
                    .filter(v -> v != null)
                    .map(d -> new TagSuggest.Suggestion(SCORE, d.id.key, d.id.value))
                    .iterator()));

            return async.resolved(TagSuggest.of(suggestions));
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

        return async.resolved(KeySuggest.of(suggestions));
    }

    @Override
    public AsyncFuture<TagValueSuggest> tagValueSuggest(final TagValueSuggest.Request request) {
        try (final Stream<TagDocument> docs = lookupTags(request.getFilter())) {
            final Stream<TagId> ids = docs.map(TagDocument::getId);

            final List<String> values = request
                .getLimit()
                .limitStream(
                    request.getKey().map(k -> ids.filter(id -> id.key.equals(k))).orElse(ids))
                .map(id -> id.value)
                .collect(Collectors.toList());

            return async.resolved(TagValueSuggest.of(values, false));
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

            return async.resolved(WriteSuggest.of());
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
        return keyIndex.values().stream().filter(e -> filter.apply(e.series)).onClose(l::unlock);
    }

    private Stream<TagDocument> lookupTags(final Filter filter) {
        final Lock l = lock.readLock();
        l.lock();
        return tagIndex.values().stream().filter(e -> filter.apply(e.series)).onClose(l::unlock);
    }

    private Stream<Series> lookupSeries(final Filter filter) {
        final Lock l = lock.readLock();
        l.lock();
        return series.stream().filter(filter::apply).onClose(l::unlock);
    }

    public String toString() {
        return "MemoryBackend()";
    }

    @Data
    static class TagId {
        private final String key;
        private final String value;
    }

    @Data
    static class KeyDocument {
        private final String id;
        private final Series series;
    }

    @Data
    static class TagDocument {
        private final TagId id;
        private final Series series;
    }
}
