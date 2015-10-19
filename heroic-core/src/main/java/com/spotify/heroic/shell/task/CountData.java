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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryOptions;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.StreamCollector;
import lombok.Data;
import lombok.ToString;

@TaskUsage("Count data for a given set of keys")
@TaskName("count-data")
public class CountData implements ShellTask {
    @Inject
    private MetricManager metrics;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Inject
    private AsyncFramework async;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final MetricBackendGroup group = metrics.useGroup(params.group);

        final QueryOptions options = QueryOptions.builder().tracing(params.tracing).build();

        final ImmutableList.Builder<BackendKey> keys = ImmutableList.builder();

        if (params.file != null) {
            try (final BufferedReader reader = new BufferedReader(
                    new InputStreamReader(io.newInputStream(params.file)))) {
                String line;

                while ((line = reader.readLine()) != null) {
                    keys.add(mapper.readValue(line.trim(), BackendKeyArgument.class).toBackendKey());
                }
            }
        }

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
                io.out().println(
                        "Finished (resolved: " + resolved + ", failed: " + failed + ", cancelled: " + cancelled + ")");
                io.out().println("Total Count: " + count.get());
                io.out().flush();
                return null;
            }

            private void check() {
                final long fin = finished.incrementAndGet();

                if (dot <= 0)
                    return;

                if (fin % dot == 0) {
                    io.out().print(".");
                    io.out().flush();
                }
            }
        }, params.parallelism);
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-f", aliases = { "--file" }, usage = "File to read keys from", metaVar = "<file>")
        private Path file;

        @Option(name = "-k", aliases = { "--key" }, usage = "Key to delete", metaVar = "<json>")
        private List<String> keys = new ArrayList<>();

        @Option(name = "-g", aliases = { "--group" }, usage = "Backend group to use", metaVar = "<group>")
        private String group = null;

        @Option(name = "--tracing", usage = "Enable extensive tracing")
        private boolean tracing = false;

        @Option(name = "--parallelism", usage = "Configure how many deletes to perform in parallel", metaVar = "<number>")
        private int parallelism = 20;
    }
}