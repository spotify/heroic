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

import com.google.inject.Inject;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.Getter;
import lombok.ToString;

@TaskUsage("Fetch series matching the given query")
@TaskName("metadata-migrate")
public class MetadataMigrate implements ShellTask {
    public static final int DOT_LIMIT = 10000;
    public static final int LINE_LIMIT = 20;

    @Inject
    private MetadataManager metadata;

    @Inject
    private QueryParser parser;

    @Inject
    private FilterFactory filters;

    @Inject
    private AsyncFramework async;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final RangeFilter filter = Tasks.setupRangeFilter(filters, parser, params);

        final MetadataBackend group = metadata.useGroup(params.group);
        final MetadataBackend target = metadata.useGroup(params.target);

        io.out().println("Migrating:");
        io.out().println("  from: " + group);
        io.out().println("    to: " + target);

        return group.countSeries(filter).lazyTransform(c -> {
            final long count = c.getCount();

            io.out().println(String.format("Migrating %d entrie(s)", count));

            if (!params.ok) {
                io.out().println("Migration stopped, use --ok to proceed");
                return null;
            }

            final AtomicInteger index = new AtomicInteger();

            final ResolvableFuture<Void> future = async.future();

            group.entries(filter).observe(AsyncObserver.<List<Series>> bind(future, entries -> {
                for (final Series s : entries) {
                    final int i = index.getAndIncrement();

                    if (i % DOT_LIMIT == 0) {
                        io.out().print(".");
                        io.out().flush();
                    }

                    if (i % (DOT_LIMIT * LINE_LIMIT) == 0) {
                        io.out().println(String.format(" %d/%d", i, count));
                        io.out().flush();
                    }

                    target.write(s, filter.getRange());
                }

                return async.resolved();
            }).onFinished(() -> {
                io.out().println(" " + String.format("%d/%d", index.get(), count) + " ended");
                io.out().flush();
            }));

            return future;
        });
    }

    @ToString
    private static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to migrate from",
                metaVar = "<metadata-group>", required = true)
        private String group;

        @Option(name = "-t", aliases = {"--target"}, usage = "Backend group to migrate to",
                metaVar = "<metadata-group>", required = true)
        private String target;

        @Option(name = "--ok", usage = "Verify the migration")
        private boolean ok = false;

        @Option(name = "--limit", aliases = {"--limit"},
                usage = "Limit the number of printed entries")
        @Getter
        private int limit = 10;

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }
}
