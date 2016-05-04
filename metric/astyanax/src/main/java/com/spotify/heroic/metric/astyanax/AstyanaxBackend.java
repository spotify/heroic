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

import com.google.common.base.Stopwatch;
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
import com.netflix.astyanax.serializers.IntegerSerializer;
import com.netflix.astyanax.util.RangeBuilder;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.concurrrency.ReadWriteThreadPools;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.statistics.MetricBackendReporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.NotImplementedException;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@ToString(of = {})
public class AstyanaxBackend extends AbstractMetricBackend implements LifeCycles {
    private static final QueryTrace.Identifier FETCH_SEGMENT =
        QueryTrace.identifier(AstyanaxBackend.class, "fetch_segment");
    private static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(AstyanaxBackend.class, "fetch");

    private static final MetricsRowKeySerializer KEY_SERIALIZER = MetricsRowKeySerializer.get();

    private static final ColumnFamily<MetricsRowKey, Integer> METRICS_CF =
        new ColumnFamily<MetricsRowKey, Integer>("metrics", KEY_SERIALIZER,
            IntegerSerializer.get());

    private final AsyncFramework async;
    private final ReadWriteThreadPools pools;
    private final MetricBackendReporter reporter;
    private final Groups groups;

    @Inject
    public AstyanaxBackend(
        final AsyncFramework async, final ReadWriteThreadPools pools,
        final MetricBackendReporter reporter, final Groups groups, LifeCycleRegistry registry
    ) {
        super(async);
        this.async = async;
        this.pools = pools;
        this.reporter = reporter;
        this.groups = groups;
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

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

        final Callable<WriteResult> resolver = () -> {
            final Context ctx = k.get();

            final MutationBatch mutation =
                ctx.client.prepareMutationBatch().setConsistencyLevel(ConsistencyLevel.CL_ANY);

            final Map<MetricsRowKey, ColumnListMutation<Integer>> batches =
                new HashMap<MetricsRowKey, ColumnListMutation<Integer>>();

            for (final WriteMetric write : writes) {
                final MetricCollection g = write.getData();

                if (g.getType() != MetricType.POINT) {
                    continue;
                }

                for (final Metric t : g.getData()) {
                    final Point d = (Point) t;
                    final long base = MetricsRowKeySerializer.getBaseTimestamp(d.getTimestamp());
                    final MetricsRowKey rowKey = new MetricsRowKey(write.getSeries(), base);

                    ColumnListMutation<Integer> m = batches.get(rowKey);

                    if (m == null) {
                        m = mutation.withRow(METRICS_CF, rowKey);
                        batches.put(rowKey, m);
                    }

                    m.putColumn(MetricsRowKeySerializer.calculateColumnKey(d.getTimestamp()),
                        d.getValue());
                }
            }

            final long start = System.nanoTime();
            mutation.execute();
            return WriteResult.of(ImmutableList.of(System.nanoTime() - start));
        };

        return async
            .call(resolver, pools.write())
            .onDone(reporter.reportWriteBatch())
            .onFinished(k::release);
    }

    @Override
    public AsyncFuture<FetchData> fetch(
        MetricType source, Series series, final DateRange range, FetchQuotaWatcher watcher,
        QueryOptions options
    ) {
        if (source == MetricType.POINT) {
            return fetchDataPoints(series, range, watcher);
        }

        throw new NotImplementedException("unsupported source: " + source);
    }

    private AsyncFuture<FetchData> fetchDataPoints(
        final Series series, DateRange range, final FetchQuotaWatcher watcher
    ) {
        return context.doto(ctx -> {
            return async.resolved(prepareQueries(series, range)).lazyTransform(result -> {
                final List<AsyncFuture<FetchData>> queries = new ArrayList<>();

                for (final PreparedQuery q : result) {
                    queries.add(async.call(new Callable<FetchData>() {
                        @Override
                        public FetchData call() throws Exception {
                            final Stopwatch w = Stopwatch.createStarted();

                            final RowQuery<MetricsRowKey, Integer> query = ctx.client
                                .prepareQuery(METRICS_CF)
                                .getRow(q.rowKey)
                                .autoPaginate(true)
                                .withColumnRange(q.columnRange);

                            final List<Point> data =
                                q.rowKey.buildPoints(query.execute().getResult());

                            if (!watcher.readData(data.size())) {
                                throw new IllegalArgumentException("data limit quota violated");
                            }

                            final QueryTrace trace =
                                new QueryTrace(FETCH_SEGMENT, w.elapsed(TimeUnit.NANOSECONDS));
                            final List<Long> times = ImmutableList.of(trace.getElapsed());
                            final List<MetricCollection> groups =
                                ImmutableList.of(MetricCollection.points(data));
                            return new FetchData(series, times, groups, trace);
                        }
                    }, pools.read()).onDone(reporter.reportFetch()));
                }

                return async.collect(queries, FetchData.collect(FETCH, series));
            });
        });
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
                final Iterator<Row<MetricsRowKey, Integer>> iterator =
                    result.getResult().iterator();

                return new Iterator<BackendEntry>() {
                    @Override
                    public boolean hasNext() {
                        final boolean next = iterator.hasNext();

                        if (!next) {
                            ctx.release();
                        }

                        return next;
                    }

                    @Override
                    public BackendEntry next() {
                        final Row<MetricsRowKey, Integer> entry = iterator.next();
                        final MetricsRowKey rowKey = entry.getKey();
                        final Series series = rowKey.getSeries();

                        final List<Point> points = rowKey.buildPoints(entry.getColumns());
                        return new BackendEntry(series, MetricCollection.points(points));
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
            if (modified.isEmpty()) {
                continue;
            }

            final MetricsRowKey rowKey = new MetricsRowKey(series, base);
            final int startColumnKey =
                MetricsRowKeySerializer.calculateColumnKey(modified.getStart());
            final int endColumnKey = MetricsRowKeySerializer.calculateColumnKey(modified.getEnd());

            final ByteBufferRange columnRange =
                new RangeBuilder().setStart(startColumnKey).setEnd(endColumnKey).build();

            prepared.add(new PreparedQuery(rowKey, columnRange));
        }

        return prepared;
    }

    @RequiredArgsConstructor
    private static final class PreparedQuery {
        private final MetricsRowKey rowKey;
        private final ByteBufferRange columnRange;
    }

    private AsyncFuture<Void> start() {
        return context.start();
    }

    private AsyncFuture<Void> stop() {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();
        futures.add(context.stop());
        futures.add(pools.stop());
        return async.collectAndDiscard(futures);
    }
}
