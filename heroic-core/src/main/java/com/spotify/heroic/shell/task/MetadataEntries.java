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
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.Entries;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.shell.task.parameters.MetadataEntriesParameters;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import java.util.function.Consumer;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TaskUsage("Fetch series matching the given query")
@TaskName("metadata-entries")
public class MetadataEntries implements ShellTask {
    private static final Logger log = LoggerFactory.getLogger(MetadataEntries.class);
    private final MetadataManager metadata;
    private final QueryParser parser;
    private final ObjectMapper mapper;
    private final AsyncFramework async;

    @Inject
    public MetadataEntries(
        MetadataManager metadata, QueryParser parser,
        @Named("application/json") ObjectMapper mapper, AsyncFramework async
    ) {
        this.metadata = metadata;
        this.parser = parser;
        this.mapper = mapper;
        this.async = async;
    }

    @Override
    public TaskParameters params() {
        return new MetadataEntriesParameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final MetadataEntriesParameters params = (MetadataEntriesParameters) base;

        final Filter filter = Tasks.setupFilter(parser, params);

        final MetadataBackend group = metadata.useOptionalGroup(params.getGroup());

        final Consumer<Series> printer;

        if (!params.getAnalytics()) {
            printer = series -> {
                try {
                    io.out().println(mapper.writeValueAsString(series));
                } catch (final Exception e) {
                    log.error("Failed to print series: {}", series, e);
                }
            };
        } else {
            printer = series -> {
                try {
                    io
                        .out()
                        .println(mapper.writeValueAsString(
                            new AnalyticsSeries(series.getHashCode().toString(),
                                mapper.writeValueAsString(series))));
                } catch (final Exception e) {
                    log.error("Failed to print series: {}", series, e);
                }
            };
        }

        return group
            .countSeries(new CountSeries.Request(filter, params.getRange(), params.getLimit()))
            .lazyTransform(c -> {
                final ResolvableFuture<Void> future = async.future();

                group
                    .entries(new Entries.Request(filter, params.getRange(), params.getLimit()))
                    .observe(AsyncObserver.bind(future, entries -> {
                        entries.getSeries().forEach(printer);
                        return async.resolved();
                    }));

                return future;
            });
    }

    public static MetadataEntries setup(final CoreComponent core) {
        return DaggerMetadataEntries_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        MetadataEntries task();
    }
}
