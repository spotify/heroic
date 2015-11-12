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

package com.spotify.heroic.metric;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import lombok.Data;

@Data
public class TagValues {
    private final String key;
    private final List<String> values;

    @JsonCreator
    public TagValues(@JsonProperty("key") String key, @JsonProperty("values") List<String> values) {
        this.key = key;
        this.values = values;
    }

    public Iterator<Map.Entry<String, String>> iterator() {
        final Iterator<String> values = this.values.iterator();

        return new Iterator<Map.Entry<String, String>>() {
            @Override
            public boolean hasNext() {
                return values.hasNext();
            }

            @Override
            public Map.Entry<String, String> next() {
                return Pair.of(key, values.next());
            }
        };
    }

    private static final Comparator<String> COMPARATOR = new Comparator<String>() {
        @Override
        public int compare(String a, String b) {
            if (a == null) {
                if (b == null) {
                    return 0;
                }

                return -1;
            }

            if (b == null) {
                return 1;
            }

            return a.compareTo(b);
        }
    };

    public static List<TagValues> fromEntries(final Iterator<Map.Entry<String, String>> entries) {
        final Map<String, SortedSet<String>> key = new HashMap<>();

        while (entries.hasNext()) {
            final Map.Entry<String, String> e = entries.next();

            SortedSet<String> values = key.get(e.getKey());

            if (values == null) {
                values = new TreeSet<String>(COMPARATOR);
                key.put(e.getKey(), values);
            }

            values.add(e.getValue());
        }

        final List<TagValues> group = new ArrayList<>(key.size());

        for (final Map.Entry<String, SortedSet<String>> e : key.entrySet()) {
            group.add(new TagValues(e.getKey(), new ArrayList<>(e.getValue())));
        }

        return group;
    }

    public static Map<String, String> mapOfSingles(final List<TagValues> tags) {
        final ImmutableMap.Builder<String, String> map = ImmutableMap.builder();

        for (final TagValues tv : tags) {
            if (tv.getValues().size() == 1) {
                map.put(tv.getKey(), tv.getValues().iterator().next());
            }
        }

        return map.build();
    }
}

