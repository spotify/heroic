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
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindSeriesStream;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@TaskUsage("Find series using the given filters")
@TaskName("metadata-find-series")
@Slf4j
public class MetadataFindSeries implements ShellTask {
    private final MetadataManager metadata;
    private final QueryParser parser;
    private final ObjectMapper mapper;
    private final AsyncFramework async;

    @Inject
    public MetadataFindSeries(
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
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Filter filter = Tasks.setupFilter(parser, params);

        final MetadataBackend group = metadata.useOptionalGroup(params.group);

        final Consumer<Series> printer;

        switch (params.format) {
            case "dsl":
                printer = series -> io.out().println(series.toDSL());
                break;
            case "analytics":
                printer = series -> {
                    try {
                        io
                            .out()
                            .println(mapper.writeValueAsString(
                                new AnalyticsSeries(series.getHashCode().toString(),
                                    mapper.writeValueAsString(series))));
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                break;
            case "json":
                printer = series -> {
                    try {
                        io.out().println(mapper.writeValueAsString(series));
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                };
                break;
            default:
                throw new IllegalArgumentException("bad format: " + params.format);
        }

        final ResolvableFuture<Void> future = async.future();

        group
            .findSeriesStream(new FindSeries.Request(filter, params.getRange(), params.getLimit()))
            .observe(new AsyncObserver<FindSeriesStream>() {
                @Override
                public AsyncFuture<Void> observe(final FindSeriesStream value) {
                    value.getSeries().forEach(printer);
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

    @ToString
    static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private Optional<String> group = Optional.empty();

        @Option(name = "--limit", usage = "Limit the number of printed entries",
            metaVar = "<number>")
        @Getter
        private OptionalLimit limit = OptionalLimit.empty();

        @Argument
        @Getter
        private List<String> query = new ArrayList<>();

        @Option(name = "-F", aliases = {"--format"},
            usage = "Format of the output (available: json, analytics, dsl) (default: dsl)")
        private String format = "dsl";
    }

    @Data
    public static class AnalyticsSeries {
        private final String id;
        private final String series;
    }

    public static MetadataFindSeries setup(final CoreComponent core) {
        return DaggerMetadataFindSeries_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        MetadataFindSeries task();
    }
}
