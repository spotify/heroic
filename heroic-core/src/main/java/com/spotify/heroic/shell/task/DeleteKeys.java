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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.Tracing;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.StreamCollector;
import lombok.ToString;
import org.apache.commons.lang3.tuple.Pair;
import org.kohsuke.args4j.Option;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import javax.inject.Inject;
import javax.inject.Named;

@TaskUsage("Delete all data for a set of keys")
@TaskName("delete-keys")
public class DeleteKeys implements ShellTask {
    private final MetricManager metrics;
    private final ObjectMapper mapper;
    private final AsyncFramework async;

    @Inject
    public DeleteKeys(
        MetricManager metrics, @Named("application/json") ObjectMapper mapper, AsyncFramework async
    ) {
        this.metrics = metrics;
        this.mapper = mapper;
        this.async = async;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final MetricBackendGroup group = metrics.useOptionalGroup(params.group);

        final QueryOptions options =
            QueryOptions.builder().tracing(Tracing.fromBoolean(params.tracing)).build();

        Stream<BackendKey> keys = Stream.empty();

        keys = Stream.concat(keys, Tasks
            .parseJsonLines(mapper, params.file, io, BackendKeyArgument.class)
            .map(BackendKeyArgument::toBackendKey));

        final ImmutableList.Builder<BackendKey> arguments = ImmutableList.builder();

        for (final String k : params.keys) {
            arguments.add(mapper.readValue(k, BackendKeyArgument.class).toBackendKey());
        }

        keys = Stream.concat(keys, arguments.build().stream());

        if (!params.ok) {
            return askForOk(io, keys);
        }

        return doDelete(io, params, group, options, keys);
    }

    private AsyncFuture<Void> doDelete(
        final ShellIO io, final Parameters params, final MetricBackendGroup group,
        final QueryOptions options, final Stream<BackendKey> keys
    ) {
        final StreamCollector<Pair<BackendKey, Long>, Void> collector =
            new StreamCollector<Pair<BackendKey, Long>, Void>() {
                @Override
                public void resolved(Pair<BackendKey, Long> result) throws Exception {
                    if (params.verbose) {
                        synchronized (io) {
                            io
                                .out()
                                .println("Deleted: " + result.getLeft() + " (" + result.getRight() +
                                    ")");
                            io.out().flush();
                        }
                    }
                }

                @Override
                public void failed(Throwable cause) throws Exception {
                    synchronized (io) {
                        io.out().println("Delete Failed: " + cause);
                        cause.printStackTrace(io.out());
                        io.out().flush();
                    }
                }

                @Override
                public void cancelled() throws Exception {
                }

                @Override
                public Void end(int resolved, int failed, int cancelled) throws Exception {
                    io
                        .out()
                        .println("Finished (resolved: " + resolved + ", failed: " + failed + ", " +
                            "cancelled: " + cancelled + ")");
                    io.out().flush();
                    return null;
                }
            };

        final AtomicInteger outstanding = new AtomicInteger(params.parallelism);

        final Object lock = new Object();

        final ResolvableFuture<Void> future = async.future();

        final Iterator<BackendKey> it = keys.iterator();

        for (int i = 0; i < params.parallelism; i++) {
            async.call(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    final BackendKey k;

                    synchronized (lock) {
                        k = it.hasNext() ? it.next() : null;
                    }

                    if (k == null) {
                        if (outstanding.decrementAndGet() == 0) {
                            future.resolve(null);
                        }

                        return null;
                    }

                    deleteKey(group, k, options).onDone(new FutureDone<Pair<BackendKey, Long>>() {
                        @Override
                        public void failed(Throwable cause) throws Exception {
                            collector.failed(cause);
                        }

                        @Override
                        public void resolved(Pair<BackendKey, Long> result) throws Exception {
                            collector.resolved(result);
                        }

                        @Override
                        public void cancelled() throws Exception {
                            collector.cancelled();
                        }
                    }).onFinished(this::call);

                    return null;
                }
            });
        }

        return future.onFinished(keys::close);
    }

    private AsyncFuture<Void> askForOk(
        final ShellIO io, final Stream<BackendKey> keys
    ) {
        io
            .out()
            .println("Examples of keys that would have been deleted (use --ok to " + "perform):");

        keys.limit(100).forEach(k -> {
            io.out().println(k.toString());
        });

        keys.close();
        return async.resolved();
    }

    private AsyncFuture<Pair<BackendKey, Long>> deleteKey(
        final MetricBackendGroup group, final BackendKey k, final QueryOptions options
    ) {
        return group
            .countKey(k, options)
            .lazyTransform(
                count -> group.deleteKey(k, options).directTransform(v -> Pair.of(k, count)));
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-f", aliases = {"--file"}, usage = "File to read keys from",
            metaVar = "<file>")
        private Optional<Path> file = Optional.empty();

        @Option(name = "-k", aliases = {"--key"}, usage = "Key to delete", metaVar = "<json>")
        private List<String> keys = new ArrayList<>();

        @Option(name = "--ok", usage = "Really delete keys", metaVar = "<file>")
        private boolean ok = false;

        @Option(name = "--verbose", usage = "Print information about every deleted key")
        private boolean verbose = false;

        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private Optional<String> group = Optional.empty();

        @Option(name = "--tracing", usage = "Enable extensive tracing")
        private boolean tracing = false;

        @Option(name = "--parallelism", usage = "Configure how many deletes to perform in parallel",
            metaVar = "<number>")
        private int parallelism = 20;
    }

    public static DeleteKeys setup(final CoreComponent core) {
        return DaggerDeleteKeys_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        DeleteKeys task();
    }
}
