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
import eu.toolchain.async.StreamCollector;
import lombok.ToString;
import org.kohsuke.args4j.Option;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Named;

@TaskUsage("Count data for a given set of keys")
@TaskName("count-data")
public class CountData implements ShellTask {
    private final MetricManager metrics;
    private final ObjectMapper mapper;
    private final AsyncFramework async;

    @Inject
    public CountData(
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

        final ImmutableList.Builder<BackendKey> keys = ImmutableList.builder();

        Tasks
            .parseJsonLines(mapper, params.file, io, BackendKeyArgument.class)
            .map(BackendKeyArgument::toBackendKey)
            .forEach(keys::add);

        for (final String k : params.keys) {
            keys.add(mapper.readValue(k, BackendKeyArgument.class).toBackendKey());
        }

        final ImmutableList.Builder<Callable<AsyncFuture<Long>>> futures = ImmutableList.builder();

        for (final BackendKey k : keys.build()) {
            futures.add(() -> group.countKey(k, options));
        }

        final List<Callable<AsyncFuture<Long>>> f = futures.build();

        final long dot = f.size() / 100;

        return async.eventuallyCollect(f, new StreamCollector<Long, Void>() {
            final AtomicLong finished = new AtomicLong();
            final AtomicLong count = new AtomicLong();

            @Override
            public void resolved(Long result) throws Exception {
                count.addAndGet(result);

                check();
            }

            @Override
            public void failed(Throwable cause) throws Exception {
                io.out().println("Count Failed: " + cause);
                cause.printStackTrace(io.out());
                io.out().flush();

                check();
            }

            @Override
            public void cancelled() throws Exception {
                check();
            }

            @Override
            public Void end(int resolved, int failed, int cancelled) throws Exception {
                io.out().println();
                io
                    .out()
                    .println(
                        "Finished (resolved: " + resolved + ", failed: " + failed + ", cancelled:" +
                            " " + cancelled + ")");
                io.out().println("Total Count: " + count.get());
                io.out().flush();
                return null;
            }

            private void check() {
                final long fin = finished.incrementAndGet();

                if (dot <= 0) {
                    return;
                }

                if (fin % dot == 0) {
                    io.out().print(".");
                    io.out().flush();
                }
            }
        }, params.parallelism);
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-f", aliases = {"--file"}, usage = "File to read keys from",
            metaVar = "<file>")
        private Optional<Path> file = Optional.empty();

        @Option(name = "-k", aliases = {"--key"}, usage = "Key to delete", metaVar = "<json>")
        private List<String> keys = new ArrayList<>();

        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private Optional<String> group = Optional.empty();

        @Option(name = "--tracing", usage = "Enable extensive tracing")
        private boolean tracing = false;

        @Option(name = "--parallelism", usage = "Configure how many deletes to perform in parallel",
            metaVar = "<number>")
        private int parallelism = 20;
    }

    public static CountData setup(final CoreComponent core) {
        return DaggerCountData_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        CountData task();
    }
}
