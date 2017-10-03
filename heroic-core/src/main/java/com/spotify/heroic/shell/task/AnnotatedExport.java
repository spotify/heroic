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
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindSeriesStream;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.time.Clock;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.StreamCollector;
import java.io.BufferedOutputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

@TaskUsage("Export of annotated data to JSON files")
@TaskName("annotated-export")
public class AnnotatedExport implements ShellTask {
    private final AsyncFramework async;
    private final Clock clock;
    private final MetadataManager metadata;
    private final MetricManager metrics;
    private final QueryParser parser;
    private final ObjectMapper mapper;

    @Inject
    public AnnotatedExport(
        AsyncFramework async, Clock clock, MetadataManager metadata, MetricManager metrics,
        QueryParser parser, @Named("application/json") ObjectMapper mapper
    ) {
        this.async = async;
        this.clock = clock;
        this.metadata = metadata;
        this.metrics = metrics;
        this.parser = parser;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Filter filter = Tasks.setupFilter(parser, params);

        final MetadataBackend group = metadata.useDefaultGroup();
        final MetricBackendGroup m = metrics.useDefaultGroup();

        final ResolvableFuture<Void> future = async.future();

        final long now = clock.currentTimeMillis();

        final long start = params.start.map(t -> Tasks.parseInstant(t, now)).orElse(now);
        final long end = params.end
            .map(t -> Tasks.parseInstant(t, now))
            .orElseThrow(() -> new RuntimeException("end: is required"));

        final DateRange range = new DateRange(start, end);

        group
            .findSeriesStream(new FindSeries.Request(filter, params.getRange(), params.getLimit()))
            .observe(new AsyncObserver<FindSeriesStream>() {
                final ConcurrentLinkedQueue<Series> allSeries = new ConcurrentLinkedQueue<>();

                @Override
                public AsyncFuture<Void> observe(final FindSeriesStream value) {
                    allSeries.addAll(value.getSeries());
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
                    final List<Callable<AsyncFuture<SeriesAndData>>> callables = new ArrayList<>();

                    for (final Series s : allSeries) {
                        callables.add(() -> {
                            final FetchData.Request request =
                                new FetchData.Request(MetricType.POINT, s, range,
                                    QueryOptions.defaults());
                            return m
                                .fetch(request, FetchQuotaWatcher.NO_QUOTA)
                                .directTransform(data -> new SeriesAndData(s, data));
                        });
                    }

                    final StreamCollector<SeriesAndData, Void> collector =
                        new StreamCollector<SeriesAndData, Void>() {
                            @Override
                            public void resolved(final SeriesAndData result) throws Exception {
                                final Series s = result.getSeries();
                                final FetchData data = result.getData();
                                final Path path = Paths.get(String.format("%s.json", s.hash()));

                                io.out().println(path);

                                try (final PrintWriter out = new PrintWriter(
                                    new BufferedOutputStream(io.newOutputStream(path)))) {
                                    out.println(mapper.writeValueAsString(s));

                                    for (final MetricCollection c : data.getGroups()) {
                                        for (final Point p : c.getDataAs(Point.class)) {
                                            out.println(mapper.writeValueAsString(p));
                                        }
                                    }
                                }

                                io.out().flush();
                            }

                            @Override
                            public void failed(final Throwable cause) throws Exception {
                                future.fail(cause);
                            }

                            @Override
                            public void cancelled() throws Exception {
                                future.cancel();
                            }

                            @Override
                            public Void end(
                                final int resolved, final int failed, final int cancelled
                            ) throws Exception {
                                return null;
                            }
                        };

                    async
                        .eventuallyCollect(callables, collector, 20)
                        .onDone(new FutureDone<Void>() {
                            @Override
                            public void failed(final Throwable cause) throws Exception {
                                future.fail(cause);
                            }

                            @Override
                            public void resolved(final Void result) throws Exception {
                                future.resolve(result);
                            }

                            @Override
                            public void cancelled() throws Exception {
                                future.cancel();
                            }
                        });
                }
            });

        return future;
    }

    @ToString
    private static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-f", aliases = {"--file"}, usage = "File to perform test against",
            metaVar = "<file>")
        private String file = null;

        @Option(name = "--start", usage = "Start date", metaVar = "<datetime>")
        private Optional<String> start = Optional.empty();

        @Option(name = "--end", usage = "End date", metaVar = "<datetime>")
        private Optional<String> end = Optional.empty();

        @Argument
        @Getter
        private List<String> query = new ArrayList<>();

        @Override
        public OptionalLimit getLimit() {
            return OptionalLimit.empty();
        }
    }

    public static AnnotatedExport setup(final CoreComponent core) {
        return DaggerAnnotatedExport_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        AnnotatedExport task();
    }

    @Data
    private static final class SeriesAndData {
        private final Series series;
        private final FetchData data;
    }
}
