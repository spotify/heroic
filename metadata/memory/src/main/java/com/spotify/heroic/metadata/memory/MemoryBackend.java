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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.Entries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindSeriesIds;
import com.spotify.heroic.metadata.FindSeriesIdsStream;
import com.spotify.heroic.metadata.FindSeriesStream;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.WriteMetadata;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@MemoryScope
@ToString(exclude = {"async", "storage"})
public class MemoryBackend implements MetadataBackend {
    private final AsyncFramework async;
    private final Groups groups;
    private final Set<Series> storage;

    @Inject
    public MemoryBackend(
        final AsyncFramework async, final Groups groups, @Named("storage") final Set<Series> storage
    ) {
        this.async = async;
        this.groups = groups;
        this.storage = storage;
    }

    @Override
    public Groups groups() {
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
    public AsyncFuture<WriteMetadata> write(final WriteMetadata.Request request) {
        this.storage.add(request.getSeries());
        return async.resolved(WriteMetadata.of());
    }

    @Override
    public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
        final Map<String, Set<String>> tags = new HashMap<>();

        lookup(request.getFilter(), request.getLimit()).forEach(s -> {
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

        return async.resolved(FindTags.of(tags, tags.size()));
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
        final OptionalLimit limit = request.getLimit();

        final Set<Series> s =
            ImmutableSet.copyOf(lookup(request.getFilter(), limit.add(1)).iterator());

        return async.resolved(FindSeries.of(limit.limitSet(s), limit.isGreater(s.size())));
    }

    @Override
    public AsyncObservable<FindSeriesStream> findSeriesStream(
        final FindSeries.Request request
    ) {
        return observer -> {
            final OptionalLimit limit = request.getLimit();

            final FindSeriesStream result = FindSeriesStream.of(
                lookup(request.getFilter(), limit.add(1)).collect(Collectors.toSet()));

            observer.observe(result).onDone(observer.onDone());
        };
    }

    @Override
    public AsyncFuture<FindSeriesIds> findSeriesIds(final FindSeriesIds.Request request) {
        final OptionalLimit limit = request.getLimit();

        final Set<String> s =
            ImmutableSet.copyOf(lookup(request.getFilter(), limit).map(Series::hash).iterator());

        return async.resolved(FindSeriesIds.of(limit.limitSet(s), limit.isGreater(s.size())));
    }

    @Override
    public AsyncObservable<FindSeriesIdsStream> findSeriesIdsStream(
        final FindSeriesIds.Request request
    ) {
        return observer -> {
            final OptionalLimit limit = request.getLimit();

            final FindSeriesIdsStream result = FindSeriesIdsStream.of(
                lookup(request.getFilter(), limit).map(Series::hash).collect(Collectors.toSet()));

            observer.observe(result).onDone(observer.onDone());
        };
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
        return async.resolved(
            new CountSeries(ImmutableList.of(), lookupFilter(request.getFilter()).count(), false));
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
        final int deletes = (int) lookup(request.getFilter(), request.getLimit())
            .map(storage::remove)
            .filter(b -> b)
            .count();

        return async.resolved(DeleteSeries.of(deletes, 0));
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
        final Set<String> keys = ImmutableSet.copyOf(
            lookup(request.getFilter(), request.getLimit()).map(Series::getKey).iterator());

        return async.resolved(FindKeys.of(keys, keys.size(), 0));
    }

    @Override
    public AsyncObservable<Entries> entries(final Entries.Request request) {
        return observer -> observer
            .observe(new Entries(
                ImmutableList.copyOf(lookup(request.getFilter(), request.getLimit()).iterator())))
            .onFinished(observer::end);
    }

    private Stream<Series> lookupFilter(final Filter filter) {
        return storage.stream().filter(filter::apply);
    }

    private Stream<Series> lookup(final Filter filter, final OptionalLimit limit) {
        final Stream<Series> series = lookupFilter(filter);
        return limit.asLong().map(series::limit).orElse(series);
    }
}
