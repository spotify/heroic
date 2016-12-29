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
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.generator.Generator;
import com.spotify.heroic.generator.GeneratorManager;
import com.spotify.heroic.generator.MetadataGenerator;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionGroup;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.ingestion.WriteOptions;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Getter;
import lombok.ToString;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@TaskUsage("Load generated metrics into backends")
@TaskName("load-generated")
public class LoadGenerated implements ShellTask {
    private final AsyncFramework async;
    private final IngestionManager ingestion;
    private final GeneratorManager generator;
    private final MetadataGenerator metadataGenerator;
    private final ObjectMapper mapper;

    @Inject
    public LoadGenerated(
        AsyncFramework async, IngestionManager ingestion, GeneratorManager generator,
        MetadataGenerator metadataGenerator, @Named("application/json") ObjectMapper mapper
    ) {
        this.async = async;
        this.ingestion = ingestion;
        this.generator = generator;
        this.metadataGenerator = metadataGenerator;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final List<AsyncFuture<Void>> writes = new ArrayList<>();

        final List<Generator> generators = ImmutableList.copyOf(
            params.generators.stream().<Generator>map(name -> generator
                .findGenerator(name)
                .orElseThrow(
                    () -> new IllegalArgumentException("No such generator: " + name))).iterator());

        for (final Generator generator : generators) {
            final IngestionGroup group = ingestion.useOptionalGroup(params.group);

            final long now = System.currentTimeMillis();
            final long start = now - params.duration.toMilliseconds();

            final DateRange range = new DateRange(Math.max(start, 0), now);

            for (final Series s : metadataGenerator.generate(params.seriesCount)) {
                final MetricCollection c = generator.generate(s, range);

                final Ingestion.Request request =
                    new Ingestion.Request(WriteOptions.defaults(), s, c);

                writes.add(group.write(request).directTransform(n -> {
                    synchronized (io) {
                        io.out().println("Wrote: " + s);
                    }

                    return null;
                }));
            }
        }

        return async.collectAndDiscard(writes);
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private Optional<String> group = Optional.empty();

        @Option(name = "-c", aliases = {"--count"},
            usage = "The number of series to generate (default: 100)")
        @Getter
        private int seriesCount = 100;

        @Option(name = "--generator", usage = "Generator to use")
        private List<String> generators = ImmutableList.of("sine", "random-events");

        @Option(name = "-d", usage = "Duration to generate data for")
        private Duration duration = Duration.of(7, TimeUnit.DAYS);
    }

    public static LoadGenerated setup(final CoreComponent core) {
        return DaggerLoadGenerated_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        LoadGenerated task();
    }
}
