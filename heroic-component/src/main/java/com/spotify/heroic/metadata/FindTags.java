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

package com.spotify.heroic.metadata;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardError;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Data
public class FindTags {
    public static final List<RequestError> EMPTY_ERRORS = new ArrayList<>();
    public static final Map<String, Set<String>> EMPTY_TAGS = new HashMap<>();

    public static final FindTags EMPTY = new FindTags(EMPTY_ERRORS, EMPTY_TAGS, 0);

    private final List<RequestError> errors;
    private final Map<String, Set<String>> tags;
    private final int size;

    public static FindTags of(final Map<String, Set<String>> tags, final int size) {
        return new FindTags(EMPTY_ERRORS, tags, size);
    }

    public static Collector<FindTags, FindTags> reduce() {
        return results -> {
            final List<RequestError> errors = new ArrayList<>();
            final HashMap<String, Set<String>> tags = new HashMap<>();
            int size = 0;

            for (final FindTags r : results) {
                errors.addAll(r.errors);
                updateTags(tags, r.tags);
                size += r.getSize();
            }

            return new FindTags(errors, tags, size);
        };
    }

    public static Transform<Throwable, FindTags> shardError(final ClusterShard c) {
        return e -> new FindTags(ImmutableList.of(ShardError.fromThrowable(c, e)), EMPTY_TAGS, 0);
    }

    /**
     * Handle that tags is a deeply nested structure and copy it up until the closest immutable
     * type.
     */
    private static void updateTags(
        final Map<String, Set<String>> data, final Map<String, Set<String>> add
    ) {
        for (final Map.Entry<String, Set<String>> entry : add.entrySet()) {
            Set<String> entries = data.get(entry.getKey());

            if (entries == null) {
                entries = new HashSet<String>();
                data.put(entry.getKey(), entries);
            }

            entries.addAll(entry.getValue());
        }
    }

    @Data
    public static class Request {
        private final Filter filter;
        private final DateRange range;
        private final OptionalLimit limit;
    }
}
