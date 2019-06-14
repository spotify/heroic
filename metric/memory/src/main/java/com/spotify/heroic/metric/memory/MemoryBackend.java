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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.RequestTimer;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricReadResult;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import javax.inject.Inject;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
public class MemoryBackend extends AbstractMetricBackend {
    public static final String MEMORY_KEYS = "memory-keys";

    public static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(MemoryBackend.class, "fetch");

    public static final Comparator<Map.Entry<String, String>> ENTRY_COMPARATOR =
        Comparator.<Map.Entry<String, String>, String>comparing(Map.Entry::getKey).thenComparing(
            Map.Entry::getValue);

    static final List<BackendEntry> EMPTY_ENTRIES = new ArrayList<>();

    static final Comparator<SortedMap<String, String>> MAP_COMPARATOR = (a, b) -> {
        final Iterator<Map.Entry<String, String>> aIter = a.entrySet().iterator();
        final Iterator<Map.Entry<String, String>> bIter = b.entrySet().iterator();

        while (aIter.hasNext()) {
            if (!bIter.hasNext()) {
                return 1;
            }

            final int n = ENTRY_COMPARATOR.compare(aIter.next(), bIter.next());

            if (n != 0) {
                return n;
            }
        }

        if (bIter.hasNext()) {
            return -1;
        }

        return 0;
    };

    static final Comparator<MemoryKey> COMPARATOR = Comparator
        .comparing(MemoryKey::getSource)
        .thenComparing(Comparator.comparing(MemoryKey::getTags, MAP_COMPARATOR));

    private final AsyncFramework async;
    private final Groups groups;
    private final ConcurrentMap<MemoryKey, MemoryCell> storage;

    @Inject
    public MemoryBackend(final AsyncFramework async, final Groups groups) {
        super(async);
        this.async = async;
        this.groups = groups;
        this.storage = new ConcurrentHashMap<>();
    }

    @Override
    public Statistics getStatistics() {
        return Statistics.of(MEMORY_KEYS, storage.size());
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }

    @Override
    public Groups groups() {
        return groups;
    }

    @Override
    public AsyncFuture<WriteMetric> write(WriteMetric.Request request) {
        final RequestTimer<WriteMetric> timer = WriteMetric.timer();
        writeOne(request);
        return async.resolved(timer.end());
    }

    @Override
    public AsyncFuture<FetchData.Result> fetch(
        FetchData.Request request, FetchQuotaWatcher watcher,
        Consumer<MetricReadResult> metricsConsumer
    ) {
        final QueryTrace.NamedWatch w = QueryTrace.watch(FETCH);
        final MemoryKey key = new MemoryKey(request.getType(), request.getSeries().getTags());
        doFetch(key, request.getRange(), watcher, metricsConsumer);
        return async.resolved(FetchData.result(w.end()));
    }

    @Override
    public boolean isReady() {
        return true;
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        return EMPTY_ENTRIES;
    }

    @Override
    public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
        storage.remove(new MemoryKey(key.getType(), key.getSeries().getTags()));
        return async.resolved();
    }

    private void writeOne(final WriteMetric.Request request) {
        final MetricCollection g = request.getData();

        final MemoryKey key = new MemoryKey(g.getType(), request.getSeries().getTags());

        final MemoryCell cell =
            storage.computeIfAbsent(key, k -> new MemoryCell(new ConcurrentHashMap<>()));

        final ConcurrentSkipListMap<Long, Metric> metrics = cell.getEntries()
            .computeIfAbsent(request.getSeries().getResource(),
                k -> new MemoryEntry(new ConcurrentSkipListMap<>()))
            .getMetrics();

        for (final Metric d : g.data()) {
            metrics.put(d.getTimestamp(), d);
        }
    }

    private void doFetch(
        final MemoryKey key, final DateRange range, final FetchQuotaWatcher watcher,
        final Consumer<MetricReadResult> metricsConsumer
    ) {
        final MemoryCell cell = storage.get(key);

        // empty
        if (cell == null) {
            return;
        }

        for (final Map.Entry<SortedMap<String, String>, MemoryEntry> e :
            cell.getEntries().entrySet()
        ) {
            final Collection<Metric> metrics =
                e.getValue()
                    .getMetrics()
                    .subMap(range.getStart(), false, range.getEnd(), true)
                    .values();

            watcher.readData(metrics.size());

            final List<Metric> data = ImmutableList.copyOf(metrics);
            final MetricCollection collection = MetricCollection.build(key.getSource(), data);
            metricsConsumer.accept(MetricReadResult.create(collection, e.getKey()));
        }
    }

    public String toString() {
        return "MemoryBackend(groups=" + this.groups + ")";
    }
}
