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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.Bytes;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.common.Groups;
import com.spotify.heroic.common.Throwing;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.metric.AbstractMetricBackend;
import com.spotify.heroic.metric.BackendEntry;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.QueryError;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.Tracing;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.datastax.schema.Schema;
import com.spotify.heroic.metric.datastax.schema.Schema.PreparedFetch;
import com.spotify.heroic.metric.datastax.schema.SchemaBoundStatement;
import com.spotify.heroic.metric.datastax.schema.SchemaInstance;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.StreamCollector;
import eu.toolchain.async.Transform;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.inject.Inject;

import lombok.AccessLevel;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

/**
 * MetricBackend for Heroic cassandra datastore.
 */
@DatastaxScope
@Slf4j
@ToString(of = {"connection"})
public class DatastaxBackend extends AbstractMetricBackend implements LifeCycles {
    public static final QueryTrace.Identifier FETCH_SEGMENT =
        QueryTrace.identifier(DatastaxBackend.class, "fetch_segment");
    public static final QueryTrace.Identifier FETCH =
        QueryTrace.identifier(DatastaxBackend.class, "fetch");

    private final AsyncFramework async;
    private final Managed<Connection> connection;
    private final Groups groups;

    @Inject
    public DatastaxBackend(
        final AsyncFramework async, final Managed<Connection> connection, final Groups groups
    ) {
        super(async);
        this.async = async;
        this.connection = connection;
        this.groups = groups;
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    @Override
    public Groups groups() {
        return groups;
    }

    @Override
    public AsyncFuture<Void> configure() {
        return async.resolved();
    }

    @Override
    public AsyncFuture<WriteMetric> write(final WriteMetric.Request request) {
        return connection.doto(c -> doWrite(c, c.schema.writeSession(), request));
    }

    @Override
    public AsyncFuture<FetchData> fetch(
        final FetchData.Request request, final FetchQuotaWatcher watcher
    ) {
        if (!watcher.mayReadData()) {
            throw new IllegalArgumentException("query violated data limit");
        }

        final int limit = watcher.getReadDataQuota();

        return connection.doto(c -> {
            final QueryTrace.Watch w = QueryTrace.watch();

            final List<PreparedFetch> prepared =
                c.schema.ranges(request.getSeries(), request.getRange());

            if (request.getType() == MetricType.POINT) {
                List<AsyncFuture<FetchData>> fetches =
                    fetchDataPoints(w, limit, request.getOptions(), prepared, c);
                return async.collect(fetches, FetchData.collect(FETCH));
            }

            return async.resolved(FetchData.error(w.end(FETCH),
                QueryError.fromMessage("unsupported source: " + request.getType())));
        });
    }

    @Override
    public AsyncFuture<FetchData.Result> fetch(
        final FetchData.Request request, final FetchQuotaWatcher watcher,
        final Consumer<MetricCollection> metricsConsumer
    ) {
        if (!watcher.mayReadData()) {
            throw new IllegalArgumentException("query violated data limit");
        }

        final int limit = watcher.getReadDataQuota();

        return connection.doto(c -> {
            final QueryTrace.Watch w = QueryTrace.watch();

            final List<PreparedFetch> prepared =
                c.schema.ranges(request.getSeries(), request.getRange());

            if (request.getType() == MetricType.POINT) {
                List<AsyncFuture<FetchData>> fetches =
                    fetchDataPoints(w, limit, request.getOptions(), prepared, c);

                List<AsyncFuture<FetchData.Result>> results =
                    Lists.transform(fetches, fetch -> fetch.directTransform(fetchData -> {
                        fetchData.getGroups().forEach(metricsConsumer::accept);
                        return fetchData.getResult();
                    }));
                return async.collect(results, FetchData.collectResult(FETCH));
            }

            return async.resolved(FetchData.errorResult(w.end(FETCH),
                QueryError.fromMessage("unsupported source: " + request.getType())));
        });
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
    public AsyncObservable<BackendKeySet> streamKeys(
        BackendKeyFilter filter, final QueryOptions options
    ) {
        return observer -> {
            final Borrowed<Connection> b = connection.borrow();

            if (!b.isValid()) {
                observer.fail(new RuntimeException("failed to borrow connection"));
                return;
            }

            final Connection c = b.get();

            final SchemaBoundStatement select;

            try {
                select = c.schema.keyUtils().selectKeys(filter);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            prepareCachedStatement(c, select).directTransform(stmt -> {
                BoundStatement bound = stmt.bind(select.getBindings().toArray());

                if (options.getTracing().isEnabled(Tracing.DETAILED)) {
                    bound.enableTracing();
                }

                options.getFetchSize().ifPresent(bound::setFetchSize);

                final AtomicLong failedKeys = new AtomicLong();

                final AsyncObserver<List<BackendKey>> helperObserver =
                    new AsyncObserver<List<BackendKey>>() {
                        @Override
                        public AsyncFuture<Void> observe(List<BackendKey> keys) {
                            return observer.observe(
                                new BackendKeySet(keys, failedKeys.getAndSet(0)));
                        }

                        @Override
                        public void cancel() {
                            try {
                                observer.cancel();
                            } finally {
                                b.release();
                            }
                        }

                        @Override
                        public void fail(Throwable cause) {
                            try {
                                observer.fail(cause);
                            } finally {
                                b.release();
                            }
                        }

                        @Override
                        public void end() {
                            try {
                                observer.end();
                            } finally {
                                b.release();
                            }
                        }
                    };

                final RowStreamHelper<BackendKey> helper =
                    new RowStreamHelper<BackendKey>(helperObserver, c.schema.keyConverter(),
                        cause -> failedKeys.incrementAndGet());

                Async.bind(async, c.session.executeAsync(bound)).onDone(helper);
                return null;
            });
        };
    }

    @Override
    public AsyncFuture<List<String>> serializeKeyToHex(final BackendKey key) {
        final MetricsRowKey rowKey = new MetricsRowKey(key.getSeries(), key.getBase());

        return connection.doto(c -> async.resolved(
            ImmutableList.of(Bytes.toHexString(c.schema.rowKey().serialize(rowKey)))));
    }

    @Override
    public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
        return connection.doto(c -> {
            final MetricsRowKey rowKey = c.schema.rowKey().deserialize(Bytes.fromHexString(key));
            return async.resolved(
                ImmutableList.of(new BackendKey(rowKey.getSeries(), rowKey.getBase())));
        });
    }

    @Override
    public AsyncFuture<Void> deleteKey(BackendKey key, QueryOptions options) {
        return connection.doto(c -> {
            final ByteBuffer k =
                c.schema.rowKey().serialize(new MetricsRowKey(key.getSeries(), key.getBase()));
            return Async
                .bind(async, c.session.executeAsync(c.schema.deleteKey(k)))
                .directTransform(result -> null);
        });
    }

    @Override
    public AsyncFuture<Long> countKey(BackendKey key, QueryOptions options) {
        return connection.doto(c -> {
            final ByteBuffer k =
                c.schema.rowKey().serialize(new MetricsRowKey(key.getSeries(), key.getBase()));
            return Async
                .bind(async, c.session.executeAsync(c.schema.countKey(k)))
                .directTransform(result -> {
                    if (!result.isFullyFetched()) {
                        throw new IllegalStateException("Row is not fully fetched");
                    }

                    return result.iterator().next().getLong(0);
                });
        });
    }

    @Override
    public AsyncFuture<MetricCollection> fetchRow(BackendKey key) {
        return connection.doto(c -> {
            final Schema.PreparedFetch f = c.schema.row(key);

            final ResolvableFuture<MetricCollection> future = async.future();

            Async
                .bind(async, c.session.executeAsync(f.fetch(Integer.MAX_VALUE)))
                .onDone(new RowFetchHelper<>(future, f.converter(),
                    result -> async.resolved(MetricCollection.points(result.getData()))));

            return future;
        });
    }

    @Override
    public AsyncObservable<MetricCollection> streamRow(final BackendKey key) {
        return observer -> {
            final Borrowed<Connection> b = connection.borrow();

            if (!b.isValid()) {
                observer.fail(new RuntimeException("failed to borrow connection"));
                return;
            }

            final Connection c = b.get();

            final PreparedFetch f;

            try {
                f = c.schema.row(key);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            final AsyncObserver<List<Point>> helperObserver = new AsyncObserver<List<Point>>() {
                @Override
                public AsyncFuture<Void> observe(List<Point> value) {
                    return observer.observe(MetricCollection.points(value));
                }

                @Override
                public void cancel() {
                    Throwing.call(observer::cancel, b::release);
                }

                @Override
                public void fail(Throwable cause) {
                    Throwing.call(() -> observer.fail(cause), b::release);
                }

                @Override
                public void end() {
                    Throwing.call(observer::end, b::release);
                }
            };

            final RowStreamHelper<Point> helper =
                new RowStreamHelper<Point>(helperObserver, f.converter());

            Async.bind(async, c.session.executeAsync(f.fetch(Integer.MAX_VALUE))).onDone(helper);
        };
    }

    private AsyncFuture<Void> start() {
        return connection.start();
    }

    private AsyncFuture<Void> stop() {
        return connection.stop();
    }

    private final ConcurrentMap<String, AsyncFuture<PreparedStatement>> prepared =
        new ConcurrentHashMap<>();

    private final Object preparedLock = new Object();

    private AsyncFuture<PreparedStatement> prepareCachedStatement(
        final Connection c, final SchemaBoundStatement bound
    ) {
        final AsyncFuture<PreparedStatement> existing = prepared.get(bound.getStatement());

        if (existing != null) {
            return existing;
        }

        synchronized (preparedLock) {
            final AsyncFuture<PreparedStatement> sneaky = prepared.get(bound.getStatement());

            if (sneaky != null) {
                return sneaky;
            }

            final AsyncFuture<PreparedStatement> newPrepared = Async
                .bind(async, c.session.prepareAsync(bound.getStatement()))
                .directTransform(stmt -> {
                    // replace old future with a more efficient one.
                    prepared.put(bound.getStatement(), async.resolved(stmt));
                    return stmt;
                });

            prepared.put(bound.getStatement(), newPrepared);
            return newPrepared;
        }
    }

    private AsyncFuture<WriteMetric> doWrite(
        final Connection c, final SchemaInstance.WriteSession session,
        final WriteMetric.Request request
    ) throws IOException {
        final List<Callable<AsyncFuture<Long>>> callables = new ArrayList<>();

        final MetricCollection g = request.getData();

        if (g.getType() == MetricType.POINT) {
            for (final Point d : g.getDataAs(Point.class)) {
                final BoundStatement stmt = session.writePoint(request.getSeries(), d);

                callables.add(() -> {
                    final long start = System.nanoTime();
                    return Async
                        .bind(async, c.session.executeAsync(stmt))
                        .directTransform((r) -> System.nanoTime() - start);
                });
            }
        }

        return async.eventuallyCollect(callables, new StreamCollector<Long, WriteMetric>() {
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
            public WriteMetric end(int resolved, int failed, int cancelled) throws Exception {
                return new WriteMetric(ImmutableList.of(), ImmutableList.copyOf(q));
            }
        }, 500);
    }

    private AsyncFuture<QueryTrace> buildTrace(
        final Connection c, final QueryTrace.Identifier what, final long elapsed,
        List<ExecutionInfo> info
    ) {
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
                    final long eventElapsed =
                        TimeUnit.NANOSECONDS.convert(e.getSourceElapsed(), TimeUnit.MICROSECONDS);
                    children.add(QueryTrace.of(QueryTrace.identifier(e.getName()), eventElapsed));
                }

                final QueryTrace.Identifier segment = QueryTrace.identifier(
                    i.getQueriedHost().toString() + "[" + qt.getTraceId().toString() + "]");

                final long segmentElapsed =
                    TimeUnit.NANOSECONDS.convert(qt.getDurationMicros(), TimeUnit.MICROSECONDS);

                return QueryTrace.of(segment, segmentElapsed, children.build());
            }));
        }

        return async
            .collect(traces.build())
            .directTransform(t -> QueryTrace.of(what, elapsed, ImmutableList.copyOf(t)));
    }

    private List<AsyncFuture<FetchData>> fetchDataPoints(
        final QueryTrace.Watch w, final int limit, final QueryOptions options,
        final List<PreparedFetch> prepared, final Connection c
    ) throws Exception {
        final List<AsyncFuture<FetchData>> fetches = new ArrayList<>(prepared.size());

        for (final Schema.PreparedFetch p : prepared) {
            final Function<RowFetchResult<Point>, AsyncFuture<QueryTrace>> traceBuilder;

            final Statement stmt;

            if (options.getTracing().isEnabled(Tracing.DETAILED)) {
                stmt = p.fetch(limit).enableTracing();
                traceBuilder =
                    result -> buildTrace(c, FETCH_SEGMENT.extend(p.toString()), w.elapsed(),
                        result.getInfo());
            } else {
                stmt = p.fetch(limit);
                traceBuilder = result -> async.resolved(w.end(FETCH_SEGMENT));
            }

            final ResolvableFuture<FetchData> future = async.future();

            Async
                .bind(async, c.session.executeAsync(stmt))
                .onDone(new RowFetchHelper<>(future, p.converter(),
                    result -> traceBuilder.apply(result).directTransform(trace -> {
                        final ImmutableList<Long> times = ImmutableList.of(trace.getElapsed());
                        final List<MetricCollection> groups =
                            ImmutableList.of(MetricCollection.points(result.getData()));
                        return FetchData.of(trace, times, groups);
                    })));

            fetches.add(future);
        }

        return fetches;
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
                : Optional.of(
                    Async.bind(async, rows.fetchMoreResults()).directTransform(r -> null));

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
                result =
                    converter.transform(new RowFetchResult<>(rows.getAllExecutionInfo(), data));
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

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private final class RowStreamHelper<R> implements FutureDone<ResultSet> {
        private final AsyncObserver<List<R>> observer;
        private final Transform<Row, R> rowConverter;
        private final Optional<Consumer<Throwable>> errorHandler;

        public RowStreamHelper(
            AsyncObserver<List<R>> observer, Transform<Row, R> rowConverter,
            Consumer<Throwable> errorHandler
        ) {
            this(observer, rowConverter, Optional.of(errorHandler));
        }

        public RowStreamHelper(AsyncObserver<List<R>> observer, Transform<Row, R> rowConverter) {
            this(observer, rowConverter, Optional.empty());
        }

        @Override
        public void failed(Throwable cause) throws Exception {
            observer.fail(cause);
        }

        @Override
        public void cancelled() throws Exception {
            observer.cancel();
        }

        @Override
        public void resolved(final ResultSet rows) throws Exception {
            int count = rows.getAvailableWithoutFetching();

            final Optional<AsyncFuture<Void>> nextFetch = rows.isFullyFetched() ? Optional.empty()
                : Optional.of(
                    Async.bind(async, rows.fetchMoreResults()).directTransform(r -> null));

            final List<R> batch = new ArrayList<>();

            while (count-- > 0) {
                final R part;

                try {
                    part = rowConverter.transform(rows.one());
                } catch (Exception e) {
                    /* custom error handling */
                    if (errorHandler.isPresent()) {
                        try {
                            errorHandler.get().accept(e);
                        } catch (final Exception inner) {
                            inner.addSuppressed(e);
                            observer.fail(inner);
                            return;
                        }

                        continue;
                    }

                    observer.fail(e);
                    return;
                }

                batch.add(part);
            }

            observer.observe(batch).onDone(new FutureDone<Void>() {
                @Override
                public void failed(Throwable cause) throws Exception {
                    observer.fail(cause);
                }

                @Override
                public void cancelled() throws Exception {
                    observer.cancel();
                }

                @Override
                public void resolved(Void result) throws Exception {
                    if (nextFetch.isPresent()) {
                        nextFetch.get().onDone(new FutureDone<Void>() {
                            @Override
                            public void failed(Throwable cause) throws Exception {
                                RowStreamHelper.this.failed(cause);
                            }

                            @Override
                            public void cancelled() throws Exception {
                                RowStreamHelper.this.cancelled();
                            }

                            @Override
                            public void resolved(Void result) throws Exception {
                                RowStreamHelper.this.resolved(rows);
                            }
                        });

                        return;
                    }

                    observer.end();
                }
            });
        }
    }

    private static final String SELECT_EVENTS_FORMAT =
        "SELECT * FROM system_traces.events WHERE session_id = ?";

    /**
     * Custom event fetcher based on the one available in
     * {@link com.datastax.driver.core.QueryTrace}.
     * <p>
     * We roll our own since the one available is blocking :(.
     */
    private AsyncFuture<List<Event>> getEvents(final Connection c, final UUID id) {
        final ResolvableFuture<List<Event>> future = async.future();

        final Transform<Row, Event> converter = row -> {
            return new Event(row.getString("activity"), row.getUUID("event_id").timestamp(),
                row.getInet("source"), row.getInt("source_elapsed"), row.getString("thread"));
        };

        Async
            .bind(async, c.session.executeAsync(SELECT_EVENTS_FORMAT, id))
            .onDone(new RowFetchHelper<Event, List<Event>>(future, converter, result -> {
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
