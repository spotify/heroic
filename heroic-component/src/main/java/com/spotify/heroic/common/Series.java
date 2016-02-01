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
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import eu.toolchain.serializer.AutoSerialize;
import lombok.ToString;

@AutoSerialize
@ToString(of = {"key", "tags"})
public class Series implements Comparable<Series> {
    static final SortedMap<String, String> EMPTY_TAGS = ImmutableSortedMap.<String, String> of();
    static final String EMPTY_STRING = "";

    final String key;
    final SortedMap<String, String> tags;

    @AutoSerialize.Ignore
    final HashCode hashCode;

    /**
     * Package-private constructor to avoid invalid inputs.
     *
     * @param key The key of the time series.
     * @param tags The tags of the time series.
     */
    @JsonCreator
    Series(@JsonProperty("key") String key, @JsonProperty("tags") SortedMap<String, String> tags) {
        this.key = key;
        this.tags = checkNotNull(tags, "tags");
        this.hashCode = generateHash();
    }

    public String getKey() {
        return key;
    }

    public SortedMap<String, String> getTags() {
        return tags;
    }

    @JsonIgnore
    public HashCode getHashCode() {
        return hashCode;
    }

    private HashCode generateHash() {
        final Hasher hasher = Hashing.murmur3_128().newHasher();

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

    public String hash() {
        return hashCode.toString();
    }

    @Override
    public int hashCode() {
        return hashCode.asInt();
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

    static final Series empty = new Series(EMPTY_STRING, EMPTY_TAGS);

    public static Series empty() {
        return empty;
    }

    public static Series of(String key) {
        return new Series(key, EMPTY_TAGS);
    }

    public static Series of(String key, Map<String, String> tags) {
        return of(key, tags.entrySet().iterator());
    }

    public static Series of(String key, Set<Map.Entry<String, String>> entries) {
        return of(key, entries.iterator());
    }

    public static Series of(String key, Iterator<Map.Entry<String, String>> tagPairs) {
        final TreeMap<String, String> tags = new TreeMap<>();

        while (tagPairs.hasNext()) {
            final Map.Entry<String, String> pair = tagPairs.next();
            final String tk = checkNotNull(pair.getKey());
            final String tv = pair.getValue();
            tags.put(tk, tv);
        }

        return new Series(key, tags);
    }

    @Override
    public int compareTo(Series o) {
        final int k = key.compareTo(o.getKey());

        if (k != 0) {
            return k;
        }

        final Iterator<Map.Entry<String, String>> a = tags.entrySet().iterator();
        final Iterator<Map.Entry<String, String>> b = o.tags.entrySet().iterator();

        while (a.hasNext() && b.hasNext()) {
            final Map.Entry<String, String> ae = a.next();
            final Map.Entry<String, String> be = b.next();

            int kc = ae.getKey().compareTo(be.getKey());

            if (kc != 0) {
                return kc;
            }

            int kv = ae.getValue().compareTo(be.getValue());

            if (kv != 0) {
                return kv;
            }
        }

        if (a.hasNext()) {
            return 1;
        }

        if (b.hasNext()) {
            return -1;
        }

        return 0;
    }
}
