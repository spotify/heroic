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

package com.spotify.heroic.metric.datastax;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.datastax.serializer.MetricsRowKeySerializer;
import com.spotify.heroic.metric.model.BackendEntry;
import com.spotify.heroic.metric.model.BackendKey;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;
import com.spotify.heroic.statistics.MetricBackendReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.Managed;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@RequiredArgsConstructor
@ToString(of = { "connection" })
public class DatastaxBackend implements MetricBackend, LifeCycle {
    private static final MetricsRowKeySerializer keySerializer = new MetricsRowKeySerializer();

    @Inject
    private AsyncFramework async;

    @Inject
    private ReadWriteThreadPools pools;

    @Inject
    private MetricBackendReporter reporter;

    @Inject
    private Managed<Connection> connection;

    private final Set<String> groups;

    @Override
    public Set<String> getGroups() {
        return groups;
    }

    @Override
    public AsyncFuture<WriteResult> write(final WriteMetric w) {
        final Borrowed<Connection> k = connection.borrow();

        return async.call(new Callable<WriteResult>() {
            @Override
            public WriteResult call() throws Exception {
                final Connection c = k.get();
                final Map<Long, ByteBuffer> cache = new HashMap<>();
                final List<Long> times = writeDataPoints(c, cache, w);
                return new WriteResult(times);
            }

        }, pools.write()).on(k.releasing());
    }

    @Override
    public AsyncFuture<WriteResult> write(final Collection<WriteMetric> writes) {
        final Borrowed<Connection> k = connection.borrow();

        return async.call(new Callable<WriteResult>() {
            @Override
            public WriteResult call() throws Exception {
                final Connection c = k.get();
                final Map<Long, ByteBuffer> cache = new HashMap<>();
                final List<Long> times = new ArrayList<>();

                for (final WriteMetric w : writes) {
                    times.addAll(writeDataPoints(c, cache, w));
                }

                return new WriteResult(times);
            }
        }, pools.write()).on(k.releasing());
    }

    private List<Long> writeDataPoints(final Connection c, final Map<Long, ByteBuffer> cache, final WriteMetric w) {
        final List<Long> times = new ArrayList<Long>(w.getData().size());

        for (final DataPoint d : w.getData()) {
            final long base = MetricsRowKey.calculateBaseTimestamp(d.getTimestamp());

            ByteBuffer keyBlob = cache.get(base);

            if (keyBlob == null) {
                final MetricsRowKey key = new MetricsRowKey(w.getSeries(), base);
                keyBlob = keySerializer.serialize(key);
            }

            final int offset = MetricsRowKey.calculateColumnKey(d.getTimestamp());

            final long start = System.nanoTime();
            c.session.execute(c.write.bind(keyBlob.asReadOnlyBuffer(), offset, d.getValue()));
            final long diff = System.nanoTime() - start;
            times.add(diff);
        }

        return times;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends TimeData> AsyncFuture<FetchData<T>> fetch(Class<T> source, Series series, final DateRange range,
            final FetchQuotaWatcher watcher) {
        if (source == DataPoint.class) {
            final AsyncFuture<? extends FetchData<? extends TimeData>> f = fetchDataPoints(series, range, watcher);
            return (AsyncFuture<FetchData<T>>) f;
        }

        throw new IllegalArgumentException("unsupported source: " + source.getName());
    }

    private AsyncFuture<FetchData<DataPoint>> fetchDataPoints(final Series series, DateRange range,
            final FetchQuotaWatcher watcher) {
        final List<PreparedQuery> prepared = ranges(series, range);

        if (!watcher.mayReadData())
            throw new IllegalArgumentException("query violated data limit");

        final int limit = watcher.getReadDataQuota() + 1;

        final Borrowed<Connection> k = connection.borrow();

        final LazyTransform<List<PreparedQuery>, FetchData<DataPoint>> transform = new LazyTransform<List<PreparedQuery>, FetchData<DataPoint>>() {
            @Override
            public AsyncFuture<FetchData<DataPoint>> transform(List<PreparedQuery> result) throws Exception {
                final List<AsyncFuture<FetchData<DataPoint>>> queries = new ArrayList<>();

                final Connection c = k.get();

                for (final PreparedQuery q : prepared) {
                    final BoundStatement fetch = c.fetch.bind(q.keyBlob, q.startKey, q.endKey, limit);

                    queries.add(async.call(new Callable<FetchData<DataPoint>>() {
                        @Override
                        public FetchData<DataPoint> call() throws Exception {
                            if (!watcher.mayReadData())
                                throw new IllegalArgumentException("query violated data limit");

                            final List<DataPoint> result = new ArrayList<>();

                            final long start = System.nanoTime();
                            final ResultSet rows = c.session.execute(fetch);
                            final long diff = System.nanoTime() - start;

                            for (final Row row : rows) {
                                final long timestamp = MetricsRowKey.calculateAbsoluteTimestamp(q.base, row.getInt(0));
                                final double value = row.getDouble(1);
                                result.add(new DataPoint(timestamp, value));
                            }

                            if (!watcher.readData(result.size()))
                                throw new IllegalArgumentException("query violated data limit");

                            return new FetchData<DataPoint>(series, result, ImmutableList.of(diff));
                        }
                    }, pools.read()).onAny(reporter.reportFetch()));
                }

                return async.collect(queries, FetchData.<DataPoint> merger(series));
            }
        };

        return async.resolved(prepared).transform(transform).on(k.releasing());
    }

    @Override
    public AsyncFuture<Void> start() throws Exception {
        return connection.start();
    }

    @Override
    public AsyncFuture<Void> stop() throws Exception {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();
        futures.add(connection.stop());
        futures.add(pools.stop());
        return async.collectAndDiscard(futures);
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        throw new IllegalStateException("#listEntries is not supported");
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    public AsyncFuture<List<BackendKey>> keys(BackendKey start, BackendKey end, final int limit) {
        final ByteBuffer first = start == null ? null : keySerializer.serialize(new MetricsRowKey(start.getSeries(),
                start.getBase()));
        final ByteBuffer last = end == null ? null : keySerializer.serialize(new MetricsRowKey(end.getSeries(), end
                .getBase()));

        final Borrowed<Connection> k = connection.borrow();

        return async.call(new Callable<List<BackendKey>>() {
            @Override
            public List<BackendKey> call() throws Exception {
                final Connection c = k.get();
                final BoundStatement stmt = keysStatement(limit, c, first, last);
                final ResultSet rows = c.session.execute(stmt);

                final List<BackendKey> result = new ArrayList<>();

                for (final Row r : rows.all()) {
                    final ByteBuffer bytes = r.getBytes("metric_key");
                    final MetricsRowKey key = keySerializer.deserialize(bytes);
                    result.add(new BackendKey(key.getSeries(), key.getBase()));
                }

                return result;
            }
        }).on(k.releasing());
    }

    private BoundStatement keysStatement(int limit, final Connection c, final ByteBuffer first, final ByteBuffer last) {
        if (first == null && last == null)
            return c.keysUnbound.bind(limit);

        if (first != null && last == null)
            return c.keysLeftbound.bind(first, limit);

        if (first == null && last != null)
            return c.keysRightbound.bind(last, limit);

        return c.keysBound.bind(first, last, limit);
    }

    private static List<PreparedQuery> ranges(final Series series, final DateRange range) {
        final List<PreparedQuery> bases = new ArrayList<>();

        final long start = MetricsRowKey.calculateBaseTimestamp(range.getStart());
        final long end = MetricsRowKey.calculateBaseTimestamp(range.getEnd());

        for (long base = start; base <= end; base += MetricsRowKey.MAX_WIDTH) {
            final DateRange modified = range.modify(base, base + MetricsRowKey.MAX_WIDTH - 1);

            if (modified.isEmpty())
                continue;

            final MetricsRowKey key = new MetricsRowKey(series, base);
            final ByteBuffer keyBlob = keySerializer.serialize(key);
            final int startKey = MetricsRowKey.calculateColumnKey(modified.start());
            final int endKey = MetricsRowKey.calculateColumnKey(modified.end());

            bases.add(new PreparedQuery(keyBlob, startKey, endKey, base));
        }

        return bases;
    }

    @RequiredArgsConstructor
    private static final class PreparedQuery {
        private final ByteBuffer keyBlob;
        private final int startKey;
        private final int endKey;
        private final long base;
    }
}