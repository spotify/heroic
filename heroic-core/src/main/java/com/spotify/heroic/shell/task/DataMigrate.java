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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.apache.commons.lang3.tuple.Pair;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricBackends;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.Getter;
import lombok.ToString;

@TaskUsage("Migrate data from one backend to another")
@TaskName("data-migrate")
public class DataMigrate implements ShellTask {
    @Inject
    private FilterFactory filters;

    @Inject
    private QueryParser parser;

    @Inject
    private MetricManager metric;

    @Inject
    private AsyncFramework async;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters p) throws Exception {
        final Parameters params = (Parameters)p;

        final BackendKey start;

        if (params.start != null) {
            start = mapper.readValue(params.start, BackendKeyArgument.class).toBackendKey();
        } else {
            start = null;
        }

        final QueryOptions options = QueryOptions.builder().tracing(params.tracing).build();

        final Filter filter = Tasks.setupFilter(filters, parser, params);
        final MetricBackend from = metric.useGroup(params.from);
        final MetricBackend to = metric.useGroup(params.to);

        final AtomicInteger writing = new AtomicInteger();
        final AtomicInteger fetching = new AtomicInteger();

        return MetricBackends.keysPager(start, params.pageLimit, (s, l) -> from.keys(s, l, options), (set) -> {
            if (set.getTrace().isPresent()) {
                set.getTrace().get().formatTrace(io.out());
                io.out().flush();
            }
        }).lazyTransform(result -> {
            final Supplier<Optional<Pair<Integer, BackendKey>>> supplier = new Supplier<Optional<Pair<Integer, BackendKey>>>() {
                int index = 0;

                @Override
                public Optional<Pair<Integer, BackendKey>> get() {
                    synchronized (io) {
                        while (result.hasNext() && index < params.limit) {
                            final BackendKey next;

                            try {
                                next = result.next();
                            } catch (Exception e) {
                                io.out().println(index + ": Exception when pulling key: " + e);
                                e.printStackTrace(io.out());
                                continue;
                            }

                            if (!filter.apply(next.getSeries())) {
                                io.out().println(
                                        String.format("%d: Skipping key since it does not match filter (%s): %s", index,
                                                filter, next));
                                continue;
                            }

                            return Optional.of(Pair.of(index++, next));
                        }

                        return Optional.empty();
                    }
                }
            };

            final ResolvableFuture<Void> future = async.future();

            final AtomicInteger active = new AtomicInteger(params.parallelism);

            final Callable<Void> doOne = new Callable<Void>() {
                public Void call() throws Exception {
                    final Optional<Pair<Integer, BackendKey>> next = supplier.get();

                    if (!next.isPresent()) {
                        if (active.decrementAndGet() == 0) {
                            future.resolve(null);
                        }

                        return null;
                    }

                    final Pair<Integer, BackendKey> n = next.get();

                    final int index = n.getLeft();
                    final String json = mapper.writeValueAsString(n.getRight());

                    fetching.incrementAndGet();

                    from.streamRow(n.getRight()).observe(new AsyncObserver<MetricCollection>() {
                        @Override
                        public AsyncFuture<Void> observe(final MetricCollection value) throws Exception {
                            writing.incrementAndGet();
                            return to.write(new WriteMetric(n.getRight().getSeries(), value))
                                    .onFinished(writing::decrementAndGet).directTransform(v -> null);
                        }

                        @Override
                        public void cancel() throws Exception {
                            synchronized (io) {
                                io.out().println(String.format("%d: Cancelled (%d, %d): %s", index, writing.get(),
                                        fetching.get(), json));
                                io.out().flush();
                            }
                        }

                        @Override
                        public void fail(final Throwable cause) throws Exception {
                            synchronized (io) {
                                io.out().println(String.format("%d: Failed (%d, %d): %s", index, writing.get(),
                                        fetching.get(), json));
                                cause.printStackTrace(io.out());
                                io.out().flush();
                            }
                        }

                        @Override
                        public void end() throws Exception {
                            synchronized (io) {
                                io.out().println(String.format("%d: Migrated (%d, %d): %s", index, writing.get(),
                                        fetching.get(), json));
                                io.out().flush();
                            }

                            fetching.decrementAndGet();
                            call();
                        }
                    });

                    return null;
                }
            };

            for (int i = 0; i < params.parallelism; i++) {
                doOne.call();
            }

            return future;
        });
    }

    @ToString
    private static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-f", aliases = { "--from" }, usage = "Backend group to load data from", metaVar = "<group>")
        private String from;

        @Option(name = "-t", aliases = { "--to" }, usage = "Backend group to load data to", metaVar = "<group>")
        private String to;

        @Option(name = "--start", usage = "First key to migrate", metaVar = "<json>")
        private String start;

        @Option(name = "--page-limit", usage = "Limit the number metadata entries to fetch per page (default: 100)")
        @Getter
        private int pageLimit = 100;

        @Option(name = "--limit", usage = "Limit the number entries to migrate")
        @Getter
        private int limit = Integer.MAX_VALUE;

        @Option(name = "--tracing", usage = "Trace the queries for more debugging when things go wrong")
        private boolean tracing = false;

        @Option(name = "--parallelism", usage = "The number of migration requests to send in parallel (default: 100)", metaVar = "<number>")
        private int parallelism = 100;

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }
}
