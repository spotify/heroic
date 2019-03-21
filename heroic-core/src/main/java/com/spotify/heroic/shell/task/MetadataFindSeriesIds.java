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
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.FindSeriesIds;
import com.spotify.heroic.metadata.FindSeriesIdsStream;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.shell.task.parameters.MetadataFindSeriesIdParameters;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import java.util.function.Consumer;
import javax.inject.Inject;

@TaskUsage("Find series using the given filters")
@TaskName("metadata-find-series-ids")
public class MetadataFindSeriesIds implements ShellTask {
    private final MetadataManager metadata;
    private final QueryParser parser;
    private final AsyncFramework async;

    @Inject
    public MetadataFindSeriesIds(
        MetadataManager metadata, QueryParser parser, AsyncFramework async
    ) {
        this.metadata = metadata;
        this.parser = parser;
        this.async = async;
    }

    @Override
    public TaskParameters params() {
        return new MetadataFindSeriesIdParameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final MetadataFindSeriesIdParameters params = (MetadataFindSeriesIdParameters) base;

        final Filter filter = Tasks.setupFilter(parser, params);
        final MetadataBackend group = metadata.useOptionalGroup(params.getGroup());
        final Consumer<String> printer = id -> io.out().println(id);
        final ResolvableFuture<Void> future = async.future();

        group
            .findSeriesIdsStream(
                new FindSeriesIds.Request(filter, params.getRange(), params.getLimit()))
            .observe(new AsyncObserver<FindSeriesIdsStream>() {
                @Override
                public AsyncFuture<Void> observe(final FindSeriesIdsStream value) {
                    value.getIds().forEach(printer);
                    io.out().flush();
                    return async.resolved();
                }

                @Override
                public void cancel() {
                    future.cancel();
                }

                @Override
                public void fail(final Throwable cause) {
                    future.fail(cause);
                }

                @Override
                public void end() {
                    future.resolve(null);
                }
            });

        return future;
    }

    public static MetadataFindSeriesIds setup(final CoreComponent core) {
        return DaggerMetadataFindSeriesIds_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        MetadataFindSeriesIds task();
    }
}
