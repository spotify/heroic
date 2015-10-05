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

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import javax.inject.Inject;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryOptions;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.metric.datastax.schema.Schema;
import com.spotify.heroic.metric.datastax.schema.Schema.PreparedFetch;
import com.spotify.heroic.metric.datastax.schema.SchemaInstance;
import com.spotify.heroic.statistics.MetricBackendReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.StreamCollector;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@Slf4j
@ToString(of = { "connection" })
public class DatastaxBackend extends AbstractMetricBackend implements LifeCycle {
    public static final QueryTrace.Identifier KEYS = QueryTrace.identifier(DatastaxBackend.class, "keys");
    public static final QueryTrace.Identifier FETCH_SEGMENT = QueryTrace.identifier(DatastaxBackend.class, "fetch_segment");
    public static final QueryTrace.Identifier FETCH = QueryTrace.identifier(DatastaxBackend.class, "fetch");

    private final AsyncFramework async;
    private final MetricBackendReporter reporter;
    private final Managed<Connection> connection;
    private final Groups groups;

    @Inject
    public DatastaxBackend(final AsyncFramework async, final MetricBackendReporter reporter,
            final Managed<Connection> connection, final Groups groups) {
        super(async);
        this.async = async;
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
        return connection.doto(c -> {
            return doWrite(c, c.schema.writeSession(), w);
        });
    }

    @Override
    public AsyncFuture<WriteResult> write(final Collection<WriteMetric> writes) {
        return connection.doto(c -> {
            final List<AsyncFuture<WriteResult>> futures = new ArrayList<>();

            for (final WriteMetric w : writes) {
                futures.add(doWrite(c, c.schema.writeSession(), w));
            }

            return async.collect(futures, WriteResult.merger());
        });
    }

    @Override
    public AsyncFuture<FetchData> fetch(MetricType source, Series series, final DateRange range,
            final FetchQuotaWatcher watcher, final QueryOptions options) {
        if (source == MetricType.POINT) {
            return fetchDataPoints(series, range, watcher, options);
        }

        throw new IllegalArgumentException("unsupported source: " + source);
    }

    @Override
    public AsyncFuture<Void> start() {
        return connection.start();
    }

    @Override
    public AsyncFuture<Void> stop() {
        return connection.stop();
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
    public AsyncFuture<BackendKeySet> keys(BackendKey start, final int limit, final QueryOptions options) {
        return connection.doto(c -> {
            final Optional<ByteBuffer> first = start == null ? Optional.empty()
                    : Optional.of(c.schema.rowKey().serialize(new MetricsRowKey(start.getSeries(), start.getBase())));

            final Statement stmt;
            final Transform<RowFetchResult<BackendKey>, AsyncFuture<BackendKeySet>> converter;

            final Stopwatch w = Stopwatch.createStarted();

            if (options.isTracing()) {
                stmt = c.schema.keysPaging(first, limit).enableTracing();
                converter = result -> {
                    return buildTrace(c, KEYS, w.elapsed(TimeUnit.NANOSECONDS), result.info).directTransform(trace -> {
                        return new BackendKeySet(result.data, Optional.of(trace));
                    });
                };
            } else {
                stmt = c.schema.keysPaging(first, limit);
                converter = result -> {
                    final QueryTrace trace = new QueryTrace(KEYS, w.elapsed(TimeUnit.NANOSECONDS));
                    return async.resolved(new BackendKeySet(result.data, Optional.of(trace)));
                };
            }

            final ResolvableFuture<BackendKeySet> future = async.future();

            Async.bind(async, c.session.executeAsync(stmt)).onDone(
                    new RowFetchHelper<BackendKey, BackendKeySet>(future, c.schema.keyConverter(), converter));

            return future;
        });
    }

    @Override
    public AsyncFuture<List<String>> serializeKeyToHex(final BackendKey key) {
        final MetricsRowKey rowKey = new MetricsRowKey(key.getSeries(), key.getBase());

        return connection.doto(c -> {
            return async.resolved(ImmutableList.of(Bytes.toHexString(c.schema.rowKey().serialize(rowKey))));
        });
    }

    @Override
    public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
        return connection.doto(c -> {
            final MetricsRowKey rowKey = c.schema.rowKey().deserialize(Bytes.fromHexString(key));
            return async.resolved(ImmutableList.of(new BackendKey(rowKey.getSeries(), rowKey.getBase())));
        });
    }

    private AsyncFuture<WriteResult> doWrite(final Connection c, final SchemaInstance.WriteSession session, final WriteMetric w) throws IOException {
        final List<Callable<AsyncFuture<Long>>> callables = new ArrayList<>();

        for (final MetricCollection g : w.getGroups()) {
            if (g.getType() == MetricType.POINT) {
                for (final Point d : g.getDataAs(Point.class)) {
                    final BoundStatement stmt = session.writePoint(w.getSeries(), d);

                    callables.add(() -> {
                        final long start = System.nanoTime();
                        return Async.bind(async, c.session.executeAsync(stmt)).directTransform((r) -> System.nanoTime() - start);
                    });
                }
            }
        }

        return async.eventuallyCollect(callables, new StreamCollector<Long, WriteResult>() {
            final ConcurrentLinkedQueue<Long> q = new ConcurrentLinkedQueue<Long>();

            @Override
            public void resolved(Long result) throws Exception {
                q.add(result);
            }

            @Override
            public void failed(Throwable cause) throws Exception {
            }

            @Override
            public void cancelled() throws Exception {
            }

            @Override
            public WriteResult end(int resolved, int failed, int cancelled) throws Exception {
                return WriteResult.of(q);
            }
        }, 500);
    }

    private AsyncFuture<QueryTrace> buildTrace(final Connection c, final QueryTrace.Identifier ident, final long elapsed, List<ExecutionInfo> info) {
        final ImmutableList.Builder<AsyncFuture<QueryTrace>> traces = ImmutableList.builder();

        for (final ExecutionInfo i : info) {
            com.datastax.driver.core.QueryTrace qt = i.getQueryTrace();

            if (qt == null) {
                log.warn("Query trace requested, but is not available");
                continue;
            }

            traces.add(getEvents(c, qt.getTraceId()).directTransform(events -> {
                final ImmutableList.Builder<QueryTrace> children = ImmutableList.builder();

                for (final Event e : events) {
                    final long eventElapsed = TimeUnit.NANOSECONDS.convert(e.getSourceElapsed(),
                            TimeUnit.MICROSECONDS);
                    children.add(new QueryTrace(QueryTrace.identifier(e.getName()), eventElapsed));
                }

                final QueryTrace.Identifier segment = QueryTrace
                        .identifier(i.getQueriedHost().toString() + "[" + qt.getTraceId().toString() + "]");

                final long segmentElapsed = TimeUnit.NANOSECONDS.convert(qt.getDurationMicros(), TimeUnit.MICROSECONDS);

                return new QueryTrace(segment, segmentElapsed, children.build());
            }));
        }

        return async.collect(traces.build()).directTransform(t -> {
            return new QueryTrace(ident, elapsed, ImmutableList.copyOf(t));
        });
    }

    private AsyncFuture<FetchData> fetchDataPoints(final Series series, DateRange range,
            final FetchQuotaWatcher watcher, final QueryOptions options) {

        if (!watcher.mayReadData())
            throw new IllegalArgumentException("query violated data limit");

        final int limit = watcher.getReadDataQuota();

        return connection.doto(c -> {
            final List<PreparedFetch> prepared;

            try {
                prepared = c.schema.ranges(series, range);
            } catch (IOException e) {
                return async.failed(e);
            }

            final List<AsyncFuture<FetchData>> futures = new ArrayList<>();

            for (final Schema.PreparedFetch f : prepared) {
                final ResolvableFuture<FetchData> future = async.future();

                final Stopwatch w = Stopwatch.createStarted();

                final Function<RowFetchResult<Point>, AsyncFuture<QueryTrace>> traceBuilder;

                final Statement stmt;

                if (options.isTracing()) {
                    stmt = f.fetch(limit).enableTracing();
                    traceBuilder = result -> buildTrace(c, FETCH_SEGMENT.extend(f.toString()),
                            w.elapsed(TimeUnit.NANOSECONDS), result.getInfo());
                } else {
                    stmt = f.fetch(limit);
                    traceBuilder = result -> async.resolved(new QueryTrace(FETCH_SEGMENT, w.elapsed(TimeUnit.NANOSECONDS)));
                }

                Async.bind(async, c.session.executeAsync(stmt)).onDone(new RowFetchHelper<Point, FetchData>(future, f.converter(), result -> {
                    return traceBuilder.apply(result).directTransform(trace -> {
                        final ImmutableList<Long> times = ImmutableList.of(trace.getElapsed());
                        final List<MetricCollection> groups = ImmutableList.of(MetricCollection.points(result.getData()));
                        return new FetchData(series, times, groups, trace);
                    });
                }));

                futures.add(future.onDone(reporter.reportFetch()));
            }

            return async.collect(futures, FetchData.collect(FETCH, series));
        });
    }

    @RequiredArgsConstructor
    private final class RowFetchHelper<R, T> implements FutureDone<ResultSet> {
        private final List<R> data = new ArrayList<>();

        private final ResolvableFuture<T> future;
        private final Transform<Row, R> rowConverter;
        private final Transform<RowFetchResult<R>, AsyncFuture<T>> converter;

        @Override
        public void failed(Throwable cause) throws Exception {
            future.fail(cause);
        }

        @Override
        public void cancelled() throws Exception {
            future.cancel();
        }

        @Override
        public void resolved(final ResultSet rows) throws Exception {
            if (future.isDone()) {
                return;
            }

            int count = rows.getAvailableWithoutFetching();

            final Optional<AsyncFuture<Void>> nextFetch = rows.isFullyFetched() ? Optional.empty()
                    : Optional.of(Async.bind(async, rows.fetchMoreResults()));

            while (count-- > 0) {
                final R part;

                try {
                    part = rowConverter.transform(rows.one());
                } catch (Exception e) {
                    future.fail(e);
                    return;
                }

                data.add(part);
            }

            if (nextFetch.isPresent()) {
                nextFetch.get().onDone(new FutureDone<Void>() {
                    @Override
                    public void failed(Throwable cause) throws Exception {
                        RowFetchHelper.this.failed(cause);
                    }

                    @Override
                    public void cancelled() throws Exception {
                        RowFetchHelper.this.cancelled();
                    }

                    @Override
                    public void resolved(Void result) throws Exception {
                        RowFetchHelper.this.resolved(rows);
                    }
                });

                return;
            }

            final AsyncFuture<T> result;

            try {
                result = converter.transform(new RowFetchResult<>(rows.getAllExecutionInfo(), data));
            } catch (final Exception e) {
                future.fail(e);
                return;
            }

            future.onCancelled(result::cancel);

            result.onDone(new FutureDone<T>() {
                @Override
                public void failed(Throwable cause) throws Exception {
                    future.fail(cause);
                }

                @Override
                public void resolved(T result) throws Exception {
                    future.resolve(result);
                }

                @Override
                public void cancelled() throws Exception {
                    future.cancel();
                }
            });
        }
    }

    private static final String SELECT_EVENTS_FORMAT = "SELECT * FROM system_traces.events WHERE session_id = ?";

    /**
     * Custom event fetcher based on the one available in {@link com.datastax.driver.core.QueryTrace}.
     *
     * We roll our own since the one available is blocking :(.
     */
    private AsyncFuture<List<Event>> getEvents(final Connection c, final UUID id) {
        final ResolvableFuture<List<Event>> future = async.future();

        Async.bind(async, c.session.executeAsync(SELECT_EVENTS_FORMAT, id)).onDone(
                new RowFetchHelper<Event, List<Event>>(future, row -> {
                    return new Event(row.getString("activity"), row.getUUID("event_id").timestamp(),
                            row.getInet("source"), row.getInt("source_elapsed"), row.getString("thread"));
                }, result -> {
                    return async.resolved(ImmutableList.copyOf(result.getData()));
                }));

        return future;
    }

    @Data
    public static class Event {
        private final String name;
        private final long timestamp;
        private final InetAddress source;
        private final int sourceElapsed;
        private final String threadName;
    }

    @Data
    private static class RowFetchResult<T> {
        final List<ExecutionInfo> info;
        final List<T> data;
    }
}