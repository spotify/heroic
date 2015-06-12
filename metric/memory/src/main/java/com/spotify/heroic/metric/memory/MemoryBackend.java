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

package com.spotify.heroic.metric.memory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.inject.Inject;
import javax.inject.Named;

import lombok.Data;
import lombok.ToString;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.model.BackendEntry;
import com.spotify.heroic.metric.model.BackendKey;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@ToString
public class MemoryBackend implements MetricBackend, LifeCycle {
    private static final List<BackendEntry> EMPTY_ENTRIES = new ArrayList<>();

    private final ConcurrentMap<MemoryKey, NavigableMap<Long, TimeData>> storage = new ConcurrentHashMap<>();

    private final Object $create = new Object();

    @Inject
    private AsyncFramework async;

    @Inject
    @Named("groups")
    private Set<String> groups;

    @Override
    public AsyncFuture<Void> start() throws Exception {
        return async.resolved(null);
    }

    @Override
    public AsyncFuture<Void> stop() throws Exception {
        return async.resolved(null);
    }

    @Override
    public Set<String> getGroups() {
        return groups;
    }

    @Override
    public AsyncFuture<WriteResult> write(WriteMetric write) {
        final long start = System.nanoTime();
        final MemoryKey key = new MemoryKey(DataPoint.class, write.getSeries());
        final NavigableMap<Long, TimeData> tree = getOrCreate(key);

        synchronized (tree) {
            for (final TimeData d : write.getData())
                tree.put(d.getTimestamp(), d);
        }

        return async.resolved(WriteResult.of(System.nanoTime() - start));
    }

    @Override
    public AsyncFuture<WriteResult> write(Collection<WriteMetric> writes) {
        final List<Long> times = new ArrayList<>(writes.size());

        for (final WriteMetric write : writes) {
            final long start = System.nanoTime();

            final MemoryKey key = new MemoryKey(DataPoint.class, write.getSeries());
            final NavigableMap<Long, TimeData> tree = getOrCreate(key);

            synchronized (tree) {
                for (final TimeData d : write.getData())
                    tree.put(d.getTimestamp(), d);
            }

            times.add(System.nanoTime() - start);
        }

        return async.resolved(WriteResult.of(times));
    }

    @Override
    public <T extends TimeData> AsyncFuture<FetchData<T>> fetch(Class<T> source, Series series, DateRange range,
            FetchQuotaWatcher watcher) {
        final long start = System.nanoTime();

        final MemoryKey key = new MemoryKey(source, series);

        final NavigableMap<Long, ? extends TimeData> tree = storage.get(key);

        if (tree == null)
            return async.resolved(new FetchData<T>(series, ImmutableList.<T> of(), ImmutableList.<Long> of()));

        synchronized (tree) {
            final List<? extends TimeData> data = new ArrayList<>(tree.tailMap(range.getStart())
                    .headMap(range.getEnd()).values());
            return async.resolved(new FetchData<T>(series, (List<T>) data, ImmutableList.<Long> of(System.nanoTime()
                    - start)));
        }
    }

    @Override
    public AsyncFuture<List<BackendKey>> keys(BackendKey start, BackendKey end, int limit) {
        return async.resolved((List<BackendKey>) new ArrayList<BackendKey>());
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return EMPTY_ENTRIES;
    }

    @Data
    public static final class MemoryKey {
        private final Class<? extends TimeData> source;
        private final Series series;
    }

    /**
     * Get or create a new navigable map to store time data.
     *
     * @param key The key to create the map under.
     * @return An existing, or a newly created navigable map for the given key.
     */
    private NavigableMap<Long, TimeData> getOrCreate(final MemoryKey key) {
        final NavigableMap<Long, TimeData> tree = storage.get(key);

        if (tree != null)
            return tree;

        synchronized ($create) {
            final NavigableMap<Long, TimeData> checked = storage.get(key);

            if (checked != null)
                return checked;

            final NavigableMap<Long, TimeData> created = new TreeMap<>();
            storage.put(key, created);
            return created;
        }
    }
}