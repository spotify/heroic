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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.inject.Inject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.MetricTypedGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.metric.datastax.serializer.MetricsRowKeySerializer;
import com.spotify.heroic.statistics.MetricBackendReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.Managed;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@ToString(of = { "connection" })
public class DatastaxBackend extends AbstractMetricBackend implements LifeCycle {
    private static final MetricsRowKeySerializer keySerializer = new MetricsRowKeySerializer();

    private final AsyncFramework async;
    private final ReadWriteThreadPools pools;
    private final MetricBackendReporter reporter;
    private final Managed<Connection> connection;
    private final Groups groups;

    @Inject
    public DatastaxBackend(final AsyncFramework async, final ReadWriteThreadPools pools,
            final MetricBackendReporter reporter, final Managed<Connection> connection,
            final Groups groups) {
        super(async);
        this.async = async;
        this.pools = pools;
        this.reporter = reporter;
        this.connection = connection;
        this.groups = groups;
    }

    @Override
    public Groups getGroups() {
        return groups;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
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
        final List<Long> times = new ArrayList<Long>();

        for (final MetricTypedGroup g : w.getGroups()) {
            if (g.getType() == MetricType.POINT) {
                for (final Metric t : g.getData()) {
                    final Point d = (Point) t;
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
            }
        }

        return times;
    }

    @SuppressWarnings("unchecked")
    @Override
    public AsyncFuture<FetchData> fetch(MetricType source, Series series, final DateRange range,
            final FetchQuotaWatcher watcher) {
        if (source == MetricType.POINT) {
            return fetchDataPoints(series, range, watcher);
        }

        throw new IllegalArgumentException("unsupported source: " + source);
    }

    private AsyncFuture<FetchData> fetchDataPoints(final Series series, DateRange range,
            final FetchQuotaWatcher watcher) {
        final List<PreparedQuery> prepared = ranges(series, range);

        if (!watcher.mayReadData())
            throw new IllegalArgumentException("query violated data limit");

        final int limit = watcher.getReadDataQuota() + 1;

        final Borrowed<Connection> k = connection.borrow();

        final LazyTransform<List<PreparedQuery>, FetchData> transform = new LazyTransform<List<PreparedQuery>, FetchData>() {
            @Override
            public AsyncFuture<FetchData> transform(List<PreparedQuery> result) throws Exception {
                final List<AsyncFuture<FetchData>> queries = new ArrayList<>();

                final Connection c = k.get();

                for (final PreparedQuery q : prepared) {
                    final BoundStatement fetch = c.fetch.bind(q.keyBlob, q.startKey, q.endKey, limit);

                    queries.add(async.call(new Callable<FetchData>() {
                        @Override
                        public FetchData call() throws Exception {
                            if (!watcher.mayReadData())
                                throw new IllegalArgumentException("query violated data limit");

                            final List<Metric> data = new ArrayList<>();

                            final long start = System.nanoTime();
                            final ResultSet rows = c.session.execute(fetch);
                            final long diff = System.nanoTime() - start;

                            for (final Row row : rows) {
                                final long timestamp = MetricsRowKey.calculateAbsoluteTimestamp(q.base, row.getInt(0));
                                final double value = row.getDouble(1);
                                data.add(new Point(timestamp, value));
                            }

                            if (!watcher.readData(data.size()))
                                throw new IllegalArgumentException("query violated data limit");

                            final ImmutableList<Long> times = ImmutableList.of(diff);
                            final List<MetricTypedGroup> groups = ImmutableList.of(new MetricTypedGroup(MetricType.POINT,
                                    data));
                            return new FetchData(series, times, groups);
                        }
                    }, pools.read()).on(reporter.reportFetch()));
                }

                return async.collect(queries, FetchData.<Point> merger(series));
            }
        };

        return async.resolved(prepared).lazyTransform(transform).on(k.releasing());
    }

    @Override
    public AsyncFuture<Void> start() {
        return connection.start();
    }

    @Override
    public AsyncFuture<Void> stop() {
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
                final BoundStatement stmt = keysStatement(c, first, last);
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

    @Override
    public AsyncFuture<Iterator<BackendKey>> allKeys(BackendKey start, int limit) {
        final ByteBuffer first = start == null ? null
                : keySerializer.serialize(new MetricsRowKey(start.getSeries(), start.getBase()));

        final Borrowed<Connection> k = connection.borrow();
        final Connection c = k.get();
        final BoundStatement stmt = keysStatement(c, first, null);
        final ResultSet rows = c.session.execute(stmt);

        final Iterator<Row> it = rows.iterator();

        return async.resolved(new Iterator<BackendKey>() {
            @Override
            public boolean hasNext() {
                boolean hasNext = it.hasNext();

                if (!hasNext) {
                    k.release();
                }

                return hasNext;
            }

            @Override
            public BackendKey next() {
                final Row r = it.next();
                final ByteBuffer bytes = r.getBytes("metric_key");

                final MetricsRowKey key;

                try {
                    key = keySerializer.deserialize(bytes.slice());
                } catch (Exception e) {
                    throw new RuntimeException(String.format("Could not deserialize key: %s", Bytes.toHexString(bytes)),
                            e);
                }

                return new BackendKey(key.getSeries(), key.getBase());
            }
        });
    }

    @Override
    public AsyncFuture<List<String>> serializeKeyToHex(BackendKey key) {
        final MetricsRowKey rowKey = new MetricsRowKey(key.getSeries(), key.getBase());
        return async.resolved(ImmutableList.of(Bytes.toHexString(keySerializer.serialize(rowKey))));
    }

    @Override
    public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
        final MetricsRowKey rowKey = keySerializer.deserialize(Bytes.fromHexString(key));
        return async.resolved(ImmutableList.of(new BackendKey(rowKey.getSeries(), rowKey.getBase())));
    }

    private BoundStatement keysStatement(final Connection c, final ByteBuffer first, final ByteBuffer last) {
        if (first == null && last == null) {
            return c.keysUnbound.bind();
        }

        if (first != null && last == null) {
            return c.keysLeftbound.bind(first);
        }

        if (first == null && last != null) {
            return c.keysRightbound.bind(last);
        }

        return c.keysBound.bind(first, last);
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