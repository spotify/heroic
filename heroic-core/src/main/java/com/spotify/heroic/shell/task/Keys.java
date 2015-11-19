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

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyClause;
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

        final BackendKeyClause clause = Tasks.setupClause(params, mapper);

        final QueryOptions options = QueryOptions.builder().tracing(params.tracing).build();

        final MetricBackendGroup group = metrics.useGroup(params.group);

        final ResolvableFuture<Void> future = async.future();

        group.streamKeys(clause, options).observe(new AsyncObserver<List<BackendKey>>() {
            @Override
            public AsyncFuture<Void> observe(List<BackendKey> keys) throws Exception {
                for (final BackendKey key : keys) {
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

        @Override
        public List<String> getQuery() {
            return ImmutableList.of();
        }
    }
}
