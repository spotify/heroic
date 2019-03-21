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

import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.Entries;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.shell.task.parameters.MetadataMigrateParameters;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import javax.inject.Inject;

@TaskUsage("Fetch series matching the given query")
@TaskName("metadata-migrate")
public class MetadataMigrate implements ShellTask {
    public static final int DOT_LIMIT = 10000;
    public static final int LINE_LIMIT = 20;

    private final MetadataManager metadata;
    private final QueryParser parser;
    private final AsyncFramework async;

    @Inject
    public MetadataMigrate(
        MetadataManager metadata, QueryParser parser, AsyncFramework async
    ) {
        this.metadata = metadata;
        this.parser = parser;
        this.async = async;
    }

    @Override
    public TaskParameters params() {
        return new MetadataMigrateParameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final MetadataMigrateParameters params = (MetadataMigrateParameters) base;

        final Filter filter = Tasks.setupFilter(parser, params);

        final MetadataBackend group = metadata.useOptionalGroup(params.getGroup());
        final MetadataBackend target = metadata.useOptionalGroup(params.getTarget());

        io.out().println("Migrating:");
        io.out().println("  from: " + group);
        io.out().println("    to: " + target);

        return group
            .countSeries(new CountSeries.Request(filter, params.getRange(), params.getLimit()))
            .lazyTransform(c -> {
                final long count = c.getCount();

                io.out().println(String.format("Migrating %d entrie(s)", count));

                if (!params.isOk()) {
                    io.out().println("Migration stopped, use --ok to proceed");
                    return null;
                }

                final AtomicInteger index = new AtomicInteger();

                final ResolvableFuture<Void> future = async.future();

                group
                    .entries(new Entries.Request(filter, params.getRange(), params.getLimit()))
                    .observe(AsyncObserver.<Entries>bind(future, entries -> {
                        for (final Series s : entries.getSeries()) {
                            final int i = index.getAndIncrement();

                            if (i % DOT_LIMIT == 0) {
                                io.out().print(".");
                                io.out().flush();
                            }

                            if (i % (DOT_LIMIT * LINE_LIMIT) == 0) {
                                io.out().println(String.format(" %d/%d", i, count));
                                io.out().flush();
                            }

                            target.write(new WriteMetadata.Request(s, params.getRange()));
                        }

                        return async.resolved();
                    }).onFinished(() -> {
                        io
                            .out()
                            .println(" " + String.format("%d/%d", index.get(), count) + " ended");
                        io.out().flush();
                    }));

                return future;
            });
    }

    public static MetadataMigrate setup(final CoreComponent core) {
        return DaggerMetadataMigrate_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        MetadataMigrate task();
    }
}
