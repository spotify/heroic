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

package com.spotify.heroic.common;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.spotify.heroic.grammar.DSL;
import eu.toolchain.serializer.AutoSerialize;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

@AutoSerialize
public class Series implements Comparable<Series> {
    static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    static final SortedMap<String, String> EMPTY_TAGS = ImmutableSortedMap.<String, String>of();
    static final String EMPTY_STRING = "";
    static final SortedMap<String, String> EMPTY_RESOURCE = ImmutableSortedMap.of();

    final String key;
    final SortedMap<String, String> tags;
    final SortedMap<String, String> resource;

    @AutoSerialize.Ignore
    final HashCode hashCode;

    @AutoSerialize.Ignore
    final HashCode hashCodeTagOnly;


    /**
     * Package-private constructor to avoid invalid inputs.
     *
     * @param key The key of the time series.
     * @param tags The tags of the time series.
     */
    @JsonCreator
    Series(
        @JsonProperty("key") String key, @JsonProperty("tags") SortedMap<String, String> tags,
        @JsonProperty("resource") Optional<SortedMap<String, String>> resource
    ) {
        this(key, tags, resource.orElseGet(ImmutableSortedMap::of));
    }

    public Series(final String key, final SortedMap<String, String> tags) {
        this(key, tags, EMPTY_RESOURCE);
    }

    public Series(
        final String key, final SortedMap<String, String> tags,
        final SortedMap<String, String> resource
    ) {
        this.key = key;
        this.tags = checkNotNull(tags, "tags");
        this.resource = checkNotNull(resource, "resource");
        this.hashCode = generateHash();
        this.hashCodeTagOnly = generateHashTagOnly();
    }

    public String getKey() {
        return key;
    }

    public SortedMap<String, String> getTags() {
        return tags;
    }

    public SortedMap<String, String> getResource() {
        return resource;
    }

    private HashCode generateHash() {
        final Hasher hasher = HASH_FUNCTION.newHasher();

        if (key != null) {
            hasher.putString(key, Charsets.UTF_8);
        }

        for (final Map.Entry<String, String> kv : tags.entrySet()) {
            final String k = kv.getKey();
            final String v = kv.getValue();

            if (k != null) {
                hasher.putString(k, Charsets.UTF_8);
            }

            if (v != null) {
                hasher.putString(v, Charsets.UTF_8);
            }
        }

        for (final Map.Entry<String, String> kv : resource.entrySet()) {
          final String k = kv.getKey();
          final String v = kv.getValue();

          if (k != null) {
            hasher.putString(k, Charsets.UTF_8);
          }

          if (v != null) {
            hasher.putString(v, Charsets.UTF_8);
          }
        }

      return hasher.hash();
    }

    public String hash() {
        return hashCode.toString();
    }

    @Override
    public int hashCode() {
        return hashCode.asInt();
    }

    @JsonIgnore
    public HashCode getHashCodeTagOnly() {
      return hashCodeTagOnly;
    }

    private HashCode generateHashTagOnly() {
      final Hasher hasher = HASH_FUNCTION.newHasher();

      if (key != null) {
        hasher.putString(key, Charsets.UTF_8);
      }

      for (final Map.Entry<String, String> kv : tags.entrySet()) {
        final String k = kv.getKey();
        final String v = kv.getValue();

        if (k != null) {
          hasher.putString(k, Charsets.UTF_8);
        }

        if (v != null) {
          hasher.putString(v, Charsets.UTF_8);
        }
      }

      return hasher.hash();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (Series.class != obj.getClass()) {
            return false;
        }

        final Series o = (Series) obj;

        if (!hashCode.equals(o.hashCode)) {
            return false;
        }

        if (key == null) {
            if (o.key != null) {
                return false;
            }

            return true;
        }

        if (o.key == null) {
            return false;
        }

        if (!key.equals(o.key)) {
            return false;
        }

        if (!tags.equals(o.tags)) {
            return false;
        }

        return true;
    }

    static final Series empty = new Series(EMPTY_STRING, EMPTY_TAGS, EMPTY_RESOURCE);

    public static Series empty() {
        return empty;
    }

    public static Series of(String key) {
        return new Series(key, EMPTY_TAGS, EMPTY_RESOURCE);
    }

    public static Series of(String key, Map<String, String> tags) {
        return of(key, tags.entrySet().iterator(), EMPTY_RESOURCE.entrySet().iterator());
    }

    public static Series of(String key, Map<String, String> tags, Map<String, String> resource) {
        return of(key, tags.entrySet().iterator(), resource.entrySet().iterator());
    }

    public static Series of(String key, Set<Map.Entry<String, String>> tagEntries) {
        return of(key, tagEntries.iterator(), EMPTY_RESOURCE.entrySet().iterator());
    }

    public static Series of(
        String key, Iterator<Map.Entry<String, String>> tagPairs
    ) {
        return of(key, tagPairs, EMPTY_RESOURCE.entrySet().iterator());
    }

    public static Series of(
        String key, Iterator<Map.Entry<String, String>> tagPairs,
        Iterator<Map.Entry<String, String>> resourcePairs
    ) {
        return new Series(key, mapEntriesToSortedMap(tagPairs),
            mapEntriesToSortedMap(resourcePairs));
    }

    private static TreeMap<String, String> mapEntriesToSortedMap(
        Iterator<Entry<String, String>> mapEntries) {
        final TreeMap<String, String> treeMap = new TreeMap<>();

        while (mapEntries.hasNext()) {
            var pair = mapEntries.next();
            treeMap.put(checkNotNull(pair.getKey()), pair.getValue());
        }

        return treeMap;
    }

    public static Series of(
        String key, Map<String, String> tags, SortedMap<String, String> resource
    ) {
        return of(key, tags.entrySet().iterator(), resource.entrySet().iterator());
    }

    public Series withResource(SortedMap<String, String> resource) {
        TreeMap<String, String> mergedResourceTags = new TreeMap<>();
        mergedResourceTags.putAll(this.resource);
        mergedResourceTags.putAll(resource);
        return new Series(this.key, this.tags, mergedResourceTags);
    }

    @Override
    public int compareTo(Series o) {
        final int k = key.compareTo(o.getKey());

        if (k != 0) {
            return k;
        }

        final int compareTags = compareSortedMaps(this.tags, o.tags);
        if (compareTags != 0) {
            return compareTags;
        }

        final int compareResources = compareSortedMaps(this.resource, o.resource);
        if (compareResources != 0) {
            return compareResources;
        }

        return 0;
    }

    private int compareSortedMaps(
        final SortedMap<String, String> a, final SortedMap<String, String> b
    ) {
        final Iterator<Map.Entry<String, String>> aTags = a.entrySet().iterator();
        final Iterator<Map.Entry<String, String>> bTags = b.entrySet().iterator();

        while (aTags.hasNext() && bTags.hasNext()) {
            final Map.Entry<String, String> aEntry = aTags.next();
            final Map.Entry<String, String> bEntry = bTags.next();

            int kc = aEntry.getKey().compareTo(bEntry.getKey());

            if (kc != 0) {
                return kc;
            }

            int kv = aEntry.getValue().compareTo(bEntry.getValue());

            if (kv != 0) {
                return kv;
            }
        }

        if (aTags.hasNext()) {
            return 1;
        }

        if (bTags.hasNext()) {
            return -1;
        }
        return 0;
    }

    public static final Joiner TAGS_JOINER = Joiner.on(", ");

    public String toDSL() {
        final Iterator<String> tags = this.tags
            .entrySet()
            .stream()
            .map(e -> DSL.dumpString(e.getKey()) + "=" + DSL.dumpString(e.getValue()))
            .iterator();

        final Iterator<String> resource = this.resource
            .entrySet()
            .stream()
            .map(e -> DSL.dumpString(e.getKey()) + "=" + DSL.dumpString(e.getValue()))
            .iterator();

        return DSL.dumpString(key) + " " + "{" + TAGS_JOINER.join(tags) + "}" + " " + "{" +
            TAGS_JOINER.join(resource) + "}";
    }

    public String toString() {
        return "Series(key=" + this.getKey() + ", tags=" + this.getTags() + ", resource=" + this
          .getResource() + ")";
    }
}
