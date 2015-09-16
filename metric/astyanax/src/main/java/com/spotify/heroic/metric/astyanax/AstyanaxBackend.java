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

package com.spotify.heroic.metric.astyanax;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.commons.lang3.NotImplementedException;

import com.google.common.collect.ImmutableList;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ByteBufferRange;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowQuery;
import com.netflix.astyanax.serializers.DoubleSerializer;
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.util.RangeBuilder;
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
@ToString(of = {})
public class AstyanaxBackend extends AbstractMetricBackend implements LifeCycle {
    private static final MetricsRowKeySerializer KEY_SERIALIZER = MetricsRowKeySerializer.get();

    private static final ColumnFamily<MetricsRowKey, Integer> METRICS_CF = new ColumnFamily<MetricsRowKey, Integer>(
            "metrics", KEY_SERIALIZER, IntegerSerializer.get());

    private final AsyncFramework async;
    private final ReadWriteThreadPools pools;
    private final MetricBackendReporter reporter;
    private final Groups groups;

    public AstyanaxBackend(final AsyncFramework async, final ReadWriteThreadPools pools,
            final MetricBackendReporter reporter, final Groups groups) {
        super(async);
        this.async = async;
        this.pools = pools;
        this.reporter = reporter;
        this.groups = groups;
    }

    private static final ColumnFamily<Integer, String> CQL3_CF = ColumnFamily.newColumnFamily("Cql3CF",
            IntegerSerializer.get(), StringSerializer.get());

    private static final String INSERT_METRICS_CQL = "INSERT INTO metrics (metric_key, data_timestamp_offset, data_value) VALUES (?, ?, ?)";

    private Managed<Context> context;

    @Override
    public Groups getGroups() {
        return groups;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<WriteResult> write(WriteMetric write) {
        return write(ImmutableList.of(write));
    }

    @Override
    public AsyncFuture<WriteResult> write(final Collection<WriteMetric> writes) {
        final Borrowed<Context> k = context.borrow();

        final Callable<WriteResult> resolver = new Callable<WriteResult>() {
            @Override
            public WriteResult call() throws Exception {
                final Context ctx = k.get();

                final MutationBatch mutation = ctx.client.prepareMutationBatch().setConsistencyLevel(
                        ConsistencyLevel.CL_ANY);

                final Map<MetricsRowKey, ColumnListMutation<Integer>> batches = new HashMap<MetricsRowKey, ColumnListMutation<Integer>>();

                for (final WriteMetric write : writes) {
                    for (final MetricTypedGroup g : write.getGroups()) {
                        if (g.getType() != MetricType.POINT)
                            continue;

                        for (final Metric t : g.getData()) {
                            final Point d = (Point) t;
                            final long base = MetricsRowKeySerializer.getBaseTimestamp(d.getTimestamp());
                            final MetricsRowKey rowKey = new MetricsRowKey(write.getSeries(), base);

                            ColumnListMutation<Integer> m = batches.get(rowKey);

                            if (m == null) {
                                m = mutation.withRow(METRICS_CF, rowKey);
                                batches.put(rowKey, m);
                            }

                            m.putColumn(MetricsRowKeySerializer.calculateColumnKey(d.getTimestamp()), d.getValue());
                        }
                    }
                }

                final long start = System.nanoTime();
                mutation.execute();
                return WriteResult.of(ImmutableList.of(System.nanoTime() - start));
            }
        };

        return async.call(resolver, pools.write()).on(reporter.reportWriteBatch()).on(k.releasing());
    }

    /**
     * CQL3 implementation for insertions.
     *
     * TODO: I cannot figure out how to get batch insertions to work. Until then, THIS IS NOT an option because it will
     * murder performance in its sleep and steal its cookies.
     *
     * @param rowKey
     * @param datapoints
     * @return
     * @throws MetricBackendException If backend is not ready
     */
    @SuppressWarnings("unused")
    private AsyncFuture<Integer> writeCQL(final MetricsRowKey rowKey, final List<Point> datapoints) {
        final Borrowed<Context> k = context.borrow();

        return async.call(new Callable<Integer>() {
            @Override
            public Integer call() throws Exception {
                final Context ctx = k.get();

                for (final Point d : datapoints) {
                    ctx.client
                            .prepareQuery(CQL3_CF)
                            .withCql(INSERT_METRICS_CQL)
                            .asPreparedStatement()
                            .withByteBufferValue(rowKey, KEY_SERIALIZER)
                            .withByteBufferValue(MetricsRowKeySerializer.calculateColumnKey(d.getTimestamp()),
                                    IntegerSerializer.get()).withByteBufferValue(d.getValue(), DoubleSerializer.get())
                            .execute();
                }

                return datapoints.size();
            }
        }, pools.read()).on(k.releasing());
    }

    @SuppressWarnings("unchecked")
    @Override
    public AsyncFuture<FetchData> fetch(MetricType source, Series series, final DateRange range,
            FetchQuotaWatcher watcher) {
        if (source == MetricType.POINT) {
            return fetchDataPoints(series, range, watcher);
        }

        throw new NotImplementedException("unsupported source: " + source);
    }

    private AsyncFuture<FetchData> fetchDataPoints(final Series series, DateRange range,
            final FetchQuotaWatcher watcher) {
        final List<PreparedQuery> queries = prepareQueries(series, range);

        final Borrowed<Context> k = context.borrow();

        return async.resolved(queries).lazyTransform(new LazyTransform<List<PreparedQuery>, FetchData>() {
            @Override
            public AsyncFuture<FetchData> transform(List<PreparedQuery> result) throws Exception {
                final List<AsyncFuture<FetchData>> queries = new ArrayList<>();

                for (final PreparedQuery q : result) {
                    final Context ctx = k.get();

                    queries.add(async.call(new Callable<FetchData>() {
                        @Override
                        public FetchData call() throws Exception {
                            final long start = System.nanoTime();

                            final RowQuery<MetricsRowKey, Integer> query = ctx.client.prepareQuery(METRICS_CF)
                                    .getRow(q.rowKey).autoPaginate(true).withColumnRange(q.columnRange);

                            final List<Metric> data = q.rowKey.buildDataPoints(query.execute().getResult());

                            if (!watcher.readData(data.size()))
                                throw new IllegalArgumentException("data limit quota violated");

                            final List<Long> times = ImmutableList.of(System.nanoTime() - start);
                            final List<MetricTypedGroup> groups = ImmutableList.of(new MetricTypedGroup(MetricType.POINT,
                                    data));
                            return new FetchData(series, times, groups);
                        }
                    }, pools.read()).on(reporter.reportFetch()));
                }

                return async.collect(queries, FetchData.<Point> merger(series));
            }
        }).on(k.releasing());
    }

    @Override
    public AsyncFuture<List<BackendKey>> keys(BackendKey start, BackendKey end, final int limit) {
        final MetricsRowKey first = start != null ? new MetricsRowKey(start.getSeries(), start.getBase()) : null;
        final MetricsRowKey last = end != null ? new MetricsRowKey(end.getSeries(), end.getBase()) : null;

        final Borrowed<Context> k = context.borrow();

        return async.call(new Callable<List<BackendKey>>() {
            @Override
            public List<BackendKey> call() throws Exception {
                final OperationResult<Rows<MetricsRowKey, Integer>> op = k.get().client.prepareQuery(METRICS_CF)
                        .getKeyRange(first, last, null, null, limit).execute();

                final Rows<MetricsRowKey, Integer> result = op.getResult();

                final List<BackendKey> keys = new ArrayList<>(result.size());

                for (Row<MetricsRowKey, Integer> row : result) {
                    final MetricsRowKey k = row.getKey();
                    keys.add(new BackendKey(k.getSeries(), k.getBase()));
                }

                return keys;
            }
        }, pools.read()).on(k.releasing());
    }

    @Override
    public Iterable<BackendEntry> listEntries() {
        final Borrowed<Context> ctx = context.borrow();

        final OperationResult<Rows<MetricsRowKey, Integer>> result;

        try {
            result = ctx.get().client.prepareQuery(METRICS_CF).getAllRows().execute();
        } catch (final ConnectionException e) {
            throw new RuntimeException("Request failed", e);
        }

        return new Iterable<BackendEntry>() {
            @Override
            public Iterator<BackendEntry> iterator() {
                final Iterator<Row<MetricsRowKey, Integer>> iterator = result.getResult().iterator();

                return new Iterator<BackendEntry>() {
                    @Override
                    public boolean hasNext() {
                        final boolean next = iterator.hasNext();

                        if (!next)
                            ctx.release();

                        return next;
                    }

                    @Override
                    public BackendEntry next() {
                        final Row<MetricsRowKey, Integer> entry = iterator.next();
                        final MetricsRowKey rowKey = entry.getKey();
                        final Series series = rowKey.getSeries();

                        final List<Metric> dataPoints = rowKey.buildDataPoints(entry.getColumns());

                        return new BackendEntry(series, MetricType.POINT, dataPoints);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public boolean isReady() {
        return context.isReady();
    }

    private List<PreparedQuery> prepareQueries(final Series series, final DateRange range) {
        final List<PreparedQuery> prepared = new ArrayList<>();

        final long start = MetricsRowKeySerializer.getBaseTimestamp(range.getStart());
        final long end = MetricsRowKeySerializer.getBaseTimestamp(range.getEnd());

        for (long base = start; base <= end; base += MetricsRowKey.MAX_WIDTH) {
            final DateRange modified = range.modify(base, base + MetricsRowKey.MAX_WIDTH - 1);

            // ignore empty range
            if (modified.isEmpty())
                continue;

            final MetricsRowKey rowKey = new MetricsRowKey(series, base);
            final int startColumnKey = MetricsRowKeySerializer.calculateColumnKey(modified.getStart());
            final int endColumnKey = MetricsRowKeySerializer.calculateColumnKey(modified.getEnd());

            final ByteBufferRange columnRange = new RangeBuilder().setStart(startColumnKey).setEnd(endColumnKey)
                    .build();

            prepared.add(new PreparedQuery(rowKey, columnRange));
        }

        return prepared;
    }

    @RequiredArgsConstructor
    private static final class PreparedQuery {
        private final MetricsRowKey rowKey;
        private final ByteBufferRange columnRange;
    }

    @Override
    public AsyncFuture<Void> start() {
        return context.start();
    }

    @Override
    public AsyncFuture<Void> stop() {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();
        futures.add(context.stop());
        futures.add(pools.stop());
        return async.collectAndDiscard(futures);
    }
}
