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

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@TaskUsage("List available metric keys for all backends")
@TaskName("keys")
@Slf4j
public class Keys implements ShellTask {
    @Inject
    private AsyncFramework async;

    @Inject
    private MetricManager metrics;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final BackendKeyFilter keyFilter = Tasks.setupKeyFilter(params, mapper);

        final QueryOptions.Builder options = QueryOptions.builder().tracing(params.tracing);

        if (params.fetchSize != null) {
            options.fetchSize(params.fetchSize);
        }

        final MetricBackendGroup group = metrics.useGroup(params.group);

        final ResolvableFuture<Void> future = async.future();

        final AsyncObservable<BackendKeySet> observable;

        if (params.keysPaged) {
            observable = group.streamKeysPaged(keyFilter, options.build(), params.keysPageSize);
        } else {
            observable = group.streamKeys(keyFilter, options.build());
        }

        observable.observe(new AsyncObserver<BackendKeySet>() {
            final AtomicLong failedKeys = new AtomicLong();
            final AtomicLong total = new AtomicLong();

            @Override
            public AsyncFuture<Void> observe(BackendKeySet keys) throws Exception {
                failedKeys.addAndGet(keys.getFailedKeys());
                total.addAndGet(keys.getKeys().size() + keys.getFailedKeys());

                for (final BackendKey key : keys.getKeys()) {
                    io.out().println(mapper.writeValueAsString(key));
                }

                io.out().flush();
                return async.resolved();
            }

            @Override
            public void cancel() throws Exception {
                log.error("Cancelled");
                end();
            }

            @Override
            public void fail(final Throwable cause) throws Exception {
                log.warn("Exception when pulling keys", cause);
                end();
            }

            @Override
            public void end() throws Exception {
                io.out().println("Failed Keys: " + failedKeys.get() + "/" + total.get());
                future.resolve(null);
            }
        });

        return future;
    }

    @ToString
    private static class Parameters extends Tasks.KeyspaceBase {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
                metaVar = "<group>")
        private String group = null;

        @Option(name = "--tracing",
                usage = "Trace the queries for more debugging when things go wrong")
        private boolean tracing = false;

        @Option(name = "--keys-paged",
                usage = "Use the high-level paging mechanism when streaming keys")
        private boolean keysPaged = false;

        @Option(name = "--keys-page-size", usage = "Use the given page-size when paging keys")
        private int keysPageSize = 10;

        @Option(name = "--fetch-size", usage = "Use the given fetch size")
        private Integer fetchSize = null;

        @Override
        public List<String> getQuery() {
            return ImmutableList.of();
        }
    }
}
