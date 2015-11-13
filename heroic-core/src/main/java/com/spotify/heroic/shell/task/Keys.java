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

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyCriteria;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

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

        final BackendKeyCriteria criteria = setupCriteria(params);

        final QueryOptions options = QueryOptions.builder().tracing(params.tracing).build();

        final MetricBackendGroup group = metrics.useGroup(params.group);

        final ResolvableFuture<Void> future = async.future();

        group.streamKeys(criteria, options).observe(new AsyncObserver<List<BackendKey>>() {
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

    private BackendKeyCriteria setupCriteria(final Parameters params) throws Exception {
        final List<BackendKeyCriteria> criterias = new ArrayList<>();

        if (params.start != null) {
            criterias.add(BackendKeyCriteria
                    .gte(mapper.readValue(params.start, BackendKeyArgument.class).toBackendKey()));
        }

        if (params.startPercentage >= 0) {
            criterias.add(BackendKeyCriteria.gte((float) params.startPercentage / 100f));
        }

        if (params.endPercentage >= 0) {
            criterias.add(BackendKeyCriteria.lt((float) params.endPercentage / 100f));
        }

        final BackendKeyCriteria criteria = BackendKeyCriteria.and(criterias);

        if (params.limit >= 0) {
            return BackendKeyCriteria.limited(criteria, params.limit);
        }

        return criteria;
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--start", usage = "First key to list", metaVar = "<json>")
        private String start;

        @Option(name = "--start-percentage", usage = "First key to list in percentage",
                metaVar = "<int>")
        private int startPercentage = -1;

        @Option(name = "--end-percentage", usage = "Last key to list (exclusive) in percentage",
                metaVar = "<int>")
        private int endPercentage = -1;

        @Option(name = "--limit", usage = "Maximum number of keys to fetch per batch",
                metaVar = "<int>")
        private int limit = 1000;

        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
                metaVar = "<group>")
        private String group = null;

        @Option(name = "--tracing",
                usage = "Trace the queries for more debugging when things go wrong")
        private boolean tracing = false;
    }
}
