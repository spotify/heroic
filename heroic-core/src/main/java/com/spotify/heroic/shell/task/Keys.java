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

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricBackends;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.QueryOptions;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

import eu.toolchain.async.AsyncFuture;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@TaskUsage("List available metric keys for all backends")
@TaskName("keys")
@Slf4j
public class Keys implements ShellTask {
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

        final BackendKey start;

        if (params.start != null) {
            start = mapper.readValue(params.start, BackendKeyArgument.class).toBackendKey();
        } else {
            start = null;
        }

        final int limit = Math.max(1, Math.min(1000, params.limit));

        final QueryOptions options = QueryOptions.builder().tracing(params.tracing).build();

        final MetricBackendGroup group = metrics.useGroup(params.group);

        return MetricBackends.keysPager(start, limit, (s, l) -> group.keys(s, l, options), (set) -> {
            if (set.getTrace().isPresent()) {
                set.getTrace().get().formatTrace(io.out());
                io.out().flush();
            }
        }).directTransform(result -> {
            while (result.hasNext()) {
                final BackendKey next;

                try {
                    next = result.next();
                } catch (Exception e) {
                    log.warn("Exception when pulling key", e);
                    continue;
                }

                io.out().println(mapper.writeValueAsString(next));
                io.out().flush();
            }

            return null;
        });
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--start", usage = "First key to list (overrides start value from --series)", metaVar = "<json>")
        private String start;

        @Option(name = "--limit", usage = "Maximum number of keys to fetch per batch", metaVar = "<int>")
        private int limit = 10000;

        @Option(name = "--group", usage = "Backend group to use", metaVar = "<group>")
        private String group = null;

        @Option(name = "--tracing", usage = "Trace the queries for more debugging when things go wrong")
        private boolean tracing = false;
    }
}