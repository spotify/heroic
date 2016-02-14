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

package com.spotify.heroic.metadata.memory;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metric.WriteResult;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.elasticsearch.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

@RequiredArgsConstructor
@ToString(exclude = {"async", "storage"})
public class MemoryBackend implements MetadataBackend {
    private final AsyncFramework async;
    private final Groups groups;
    private final Set<Series> storage;

    @Override
    public Groups getGroups() {
        return groups;
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<WriteResult> write(Series series, DateRange range) {
        this.storage.add(series);
        return async.resolved(WriteResult.EMPTY);
    }

    @Override
    public AsyncFuture<Void> refresh() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<FindTags> findTags(RangeFilter filter) {
        final Map<String, Set<String>> tags = new HashMap<>();

        filter(filter.getFilter()).forEach(s -> {
            for (final Map.Entry<String, String> e : s.getTags().entrySet()) {
                Set<String> values = tags.get(e.getKey());

                if (values != null) {
                    values.add(e.getValue());
                    continue;
                }

                values = new HashSet<>();
                values.add(e.getValue());
                tags.put(e.getKey(), values);
            }
        });

        return async.resolved(new FindTags(tags, tags.size()));
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(RangeFilter filter) {
        final Set<Series> s = ImmutableSet.copyOf(filter(filter.getFilter()).iterator());
        return async.resolved(new FindSeries(s, s.size(), 0));
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(RangeFilter filter) {
        final long count = filter(filter.getFilter()).count();
        return async.resolved(new CountSeries(ImmutableList.of(), count, false));
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(RangeFilter filter) {
        final int deletes =
            (int) filter(filter.getFilter()).map(storage::remove).filter(b -> b).count();
        return async.resolved(new DeleteSeries(deletes, 0));
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(RangeFilter filter) {
        final Set<String> keys =
            ImmutableSet.copyOf(filter(filter.getFilter()).map(Series::getKey).iterator());
        return async.resolved(new FindKeys(keys, keys.size(), 0));
    }

    @Override
    public AsyncObservable<List<Series>> entries(RangeFilter filter) {
        return observer -> {
            observer
                .observe(ImmutableList.copyOf(filter(filter.getFilter()).iterator()))
                .onFinished(observer::end);
        };
    }

    private Stream<Series> filter(final Filter filter) {
        return storage.stream().filter(filter::apply);
    }
}
