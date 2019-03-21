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
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.SelectedGroup;
import com.spotify.heroic.common.Statistics;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.List;
import lombok.ToString;

@ToString(of = {"backends"})
public class MetadataBackendGroup implements MetadataBackend {
    private final SelectedGroup<MetadataBackend> backends;
    private final AsyncFramework async;

    @java.beans.ConstructorProperties({ "backends", "async" })
    public MetadataBackendGroup(final SelectedGroup<MetadataBackend> backends,
                                final AsyncFramework async) {
        this.backends = backends;
        this.async = async;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.collectAndDiscard(run(MetadataBackend::configure));
    }

    @Override
    public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
        return async.collect(run(b -> b.findTags(request)), FindTags.reduce());
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
        return async.collect(run(b -> b.countSeries(request)), CountSeries.reduce());
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
        return async.collect(run(v -> v.findSeries(request)),
            FindSeries.reduce(request.getLimit()));
    }

    @Override
    public AsyncFuture<FindSeriesIds> findSeriesIds(final FindSeriesIds.Request request) {
        return async.collect(run(v -> v.findSeriesIds(request)),
            FindSeriesIds.reduce(request.getLimit()));
    }

    @Override
    public AsyncObservable<FindSeriesStream> findSeriesStream(final FindSeries.Request request) {
        return AsyncObservable.chain(run(b -> b.findSeriesStream(request)));
    }

    @Override
    public AsyncObservable<FindSeriesIdsStream> findSeriesIdsStream(
        final FindSeriesIds.Request request
    ) {
        return AsyncObservable.chain(run(b -> b.findSeriesIdsStream(request)));
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
        return async.collect(run(b -> b.deleteSeries(request)), DeleteSeries.reduce());
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
        return async.collect(run(b -> b.findKeys(request)), FindKeys.reduce());
    }

    @Override
    public AsyncFuture<WriteMetadata> write(final WriteMetadata.Request request) {
        return async.collect(run(b -> b.write(request)), WriteMetadata.reduce());
    }

    @Override
    public AsyncObservable<Entries> entries(final Entries.Request request) {
        return AsyncObservable.chain(run(b -> b.entries(request)));
    }

    @Override
    public boolean isReady() {
        return backends.getMembers().stream().allMatch(MetadataBackend::isReady);
    }

    @Override
    public Groups groups() {
        return backends.groups();
    }

    @Override
    public boolean isEmpty() {
        return backends.isEmpty();
    }

    @Override
    public Statistics getStatistics() {
        Statistics s = Statistics.empty();

        for (final MetadataBackend b : backends) {
            s = s.merge(b.getStatistics());
        }

        return s;
    }

    private <T> List<T> run(InternalOperation<T> op) {
        final ImmutableList.Builder<T> results = ImmutableList.builder();

        for (final MetadataBackend b : backends) {
            try {
                results.add(op.run(b));
            } catch (final Exception e) {
                throw new RuntimeException("setting up backend operation failed", e);
            }
        }

        return results.build();
    }

    public static interface InternalOperation<T> {
        T run(MetadataBackend backend) throws Exception;
    }
}
