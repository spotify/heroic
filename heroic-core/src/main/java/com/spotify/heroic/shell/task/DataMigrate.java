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

package com.spotify.heroic.shell.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.Tracing;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.inject.Inject;
import javax.inject.Named;

@TaskUsage("Migrate data from one backend to another")
@TaskName("data-migrate")
public class DataMigrate implements ShellTask {
    public static final long DOTS = 100;
    public static final long LINES = DOTS * 20;
    public static final long ALLOWED_ERRORS = 5;
    public static final long ALLOWED_FAILED_KEYS = 100;

    private final QueryParser parser;
    private final MetricManager metric;
    private final AsyncFramework async;
    private final ObjectMapper mapper;

    @Inject
    public DataMigrate(
        QueryParser parser, MetricManager metric, AsyncFramework async,
        @Named("application/json") ObjectMapper mapper
    ) {
        this.parser = parser;
        this.metric = metric;
        this.async = async;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters p) throws Exception {
        final Parameters params = (Parameters) p;

        final QueryOptions.Builder options =
            QueryOptions.builder().tracing(Tracing.fromBoolean(params.tracing));

        params.fetchSize.ifPresent(options::fetchSize);

        final Filter filter = Tasks.setupFilter(parser, params);
        final MetricBackend from = metric.useOptionalGroup(params.from);
        final MetricBackend to = metric.useOptionalGroup(params.to);

        final BackendKeyFilter keyFilter = Tasks.setupKeyFilter(params, mapper);

        final ResolvableFuture<Void> future = async.future();

        /* all errors seen */
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        final AsyncObservable<BackendKeySet> observable;

        if (params.keysPaged) {
            observable = from.streamKeysPaged(keyFilter, options.build(), params.keysPageSize);
        } else {
            observable = from.streamKeys(keyFilter, options.build());
        }

        observable.observe(new KeyObserver(io, params, filter, from, to, future, errors));

        return future.directTransform(v -> {
            io.out().println();

            if (!errors.isEmpty()) {
                io.out().println("ERRORS: ");

                for (final Throwable t : errors) {
                    io.out().println(t.getMessage());
                    t.printStackTrace(io.out());
                }
            }

            io.out().flush();
            return null;
        });
    }

    @Data
    class KeyObserver implements AsyncObserver<BackendKeySet> {
        final ShellIO io;
        final Parameters params;
        final Filter filter;
        final MetricBackend from;
        final MetricBackend to;
        final ResolvableFuture<Void> future;
        final ConcurrentLinkedQueue<Throwable> errors;

        final Object lock = new Object();

        /**
         * must synchronize access with {@link #lock}
         */
        volatile boolean done = false;

        int pending = 0;
        ResolvableFuture<Void> next = null;

        /* a queue of the next keys to migrate */
        final ConcurrentLinkedQueue<BackendKey> current = new ConcurrentLinkedQueue<>();

        /* the total number of keys migrated */
        final AtomicLong total = new AtomicLong();

        /* the total number of failed keys */
        final AtomicLong failedKeys = new AtomicLong();

        /* the total number of keys */
        final AtomicLong totalKeys = new AtomicLong();

        @Override
        public AsyncFuture<Void> observe(final BackendKeySet set) {
            if (next != null) {
                return async.failed(new RuntimeException("next future is still set"));
            }

            failedKeys.addAndGet(set.getFailedKeys());
            totalKeys.addAndGet(set.getFailedKeys() + set.getKeys().size());

            if (errors.size() > ALLOWED_ERRORS) {
                return async.failed(new RuntimeException("too many failed migrations"));
            }

            if (failedKeys.get() > ALLOWED_FAILED_KEYS) {
                return async.failed(new RuntimeException("too many failed keys"));
            }

            if (future.isDone()) {
                return async.cancelled();
            }

            if (set.getKeys().isEmpty()) {
                return async.resolved();
            }

            current.addAll(set.getKeys());

            synchronized (lock) {
                next = async.future();

                while (true) {
                    if (pending >= params.parallelism) {
                        break;
                    }

                    final BackendKey k = current.poll();

                    if (k == null) {
                        break;
                    }

                    pending++;
                    streamOne(k);
                }

                if (pending < params.parallelism) {
                    return async.resolved();
                }

                return next;
            }
        }

        void streamOne(final BackendKey key) {
            if (!filter.apply(key.getSeries())) {
                endOne(key);
                return;
            }

            from
                .streamRow(key)
                .observe(new RowObserver(errors, to, future, key, () -> done, this::endOneRuntime));
        }

        void endOneRuntime(final BackendKey key) {
            try {
                endOne(key);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }

        void endOne(final BackendKey key) {
            streamDot(io, key, total.incrementAndGet());

            // opportunistically pick up the next available task without locking (if available).
            final BackendKey k = current.poll();

            if (k != null) {
                streamOne(k);
                return;
            }

            synchronized (lock) {
                pending--;

                if (next != null) {
                    final ResolvableFuture<Void> tmp = next;
                    next = null;
                    tmp.resolve(null);
                }

                checkFinished();
            }
        }

        @Override
        public void cancel() {
            synchronized (io) {
                io.out().println("Cancelled when reading keys");
            }

            end();
        }

        @Override
        public void fail(final Throwable cause) {
            synchronized (io) {
                io.out().println("Error when reading keys: " + cause.getMessage());
                cause.printStackTrace(io.out());
                io.out().flush();
            }

            end();
        }

        @Override
        public void end() {
            synchronized (lock) {
                done = true;
                checkFinished();
            }
        }

        void checkFinished() {
            if (done && pending == 0) {
                future.resolve(null);
            }
        }

        void streamDot(final ShellIO io, final BackendKey key, final long n) {
            if (n % LINES == 0) {
                synchronized (io) {
                    try {
                        io.out().println(" failedKeys: " + failedKeys.get() + ", last: " +
                            mapper.writeValueAsString(key));
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }

                    io.out().flush();
                }
            } else if (n % DOTS == 0) {
                synchronized (io) {
                    io.out().print(".");
                    io.out().flush();
                }
            }
        }
    }

    @Data
    class RowObserver implements AsyncObserver<MetricCollection> {
        final ConcurrentLinkedQueue<Throwable> errors;
        final MetricBackend to;
        final ResolvableFuture<Void> future;
        final BackendKey key;
        final Supplier<Boolean> done;
        final Consumer<BackendKey> end;

        @Override
        public AsyncFuture<Void> observe(MetricCollection value) {
            if (future.isDone() || done.get()) {
                return async.cancelled();
            }

            final AsyncFuture<Void> write = to
                .write(new WriteMetric.Request(key.getSeries(), value))
                .directTransform(v -> null);

            future.bind(write);
            return write;
        }

        @Override
        public void cancel() {
            end();
        }

        @Override
        public void fail(Throwable cause) {
            errors.add(cause);
            end();
        }

        @Override
        public void end() {
            end.accept(key);
        }
    }

    @ToString
    private static class Parameters extends Tasks.KeyspaceBase {
        @Option(name = "-f", aliases = {"--from"}, usage = "Backend group to load data from",
            metaVar = "<group>")
        private Optional<String> from = Optional.empty();

        @Option(name = "-t", aliases = {"--to"}, usage = "Backend group to load data to",
            metaVar = "<group>")
        private Optional<String> to = Optional.empty();

        @Option(name = "--page-limit",
            usage = "Limit the number metadata entries to fetch per page (default: 100)")
        @Getter
        private int pageLimit = 100;

        @Option(name = "--keys-paged",
            usage = "Use the high-level paging mechanism when streaming keys")
        private boolean keysPaged = false;

        @Option(name = "--keys-page-size", usage = "Use the given page-size when paging keys")
        private int keysPageSize = 10;

        @Option(name = "--fetch-size", usage = "Use the given fetch size")
        private Optional<Integer> fetchSize = Optional.empty();

        @Option(name = "--tracing",
            usage = "Trace the queries for more debugging when things go wrong")
        private boolean tracing = false;

        @Option(name = "--parallelism",
            usage = "The number of migration requests to send in parallel (default: 100)",
            metaVar = "<number>")
        private int parallelism = Runtime.getRuntime().availableProcessors() * 4;

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }

    public static DataMigrate setup(final CoreComponent core) {
        return DaggerDataMigrate_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        DataMigrate task();
    }
}
