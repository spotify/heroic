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
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.RateLimiter;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.task.parameters.MetadataLoadParameters;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.WriteSuggest;
import com.spotify.heroic.time.Clock;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.zip.GZIPInputStream;
import javax.inject.Inject;
import javax.inject.Named;

@TaskUsage("Load metadata from a file")
@TaskName("metadata-load")
public class MetadataLoad implements ShellTask {
    protected static final long OUTPUT_STEP = 1000;

    private final Clock clock;
    private final AsyncFramework async;
    private final SuggestManager suggest;
    private final ObjectMapper mapper;

    @Inject
    public MetadataLoad(
        Clock clock, AsyncFramework async, SuggestManager suggest,
        @Named("application/json") ObjectMapper mapper
    ) {
        this.clock = clock;
        this.async = async;
        this.suggest = suggest;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new MetadataLoadParameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final MetadataLoadParameters params = (MetadataLoadParameters) base;

        final SuggestBackend target = suggest.useGroup(params.getTarget());

        final Optional<RateLimiter> rateLimiter = params.getRate() <= 0 ? Optional.absent()
            : Optional.of(RateLimiter.create(params.getRate()));

        io.out().println("Loading suggest data:");
        io.out().println("  from (file): " + params.getFile());
        io.out().println("  to  (suggest): " + target);
        io.out().println("  rate-limit:" +
                         (rateLimiter.isPresent() ? params.getRate() : "disabled"));
        io.out().flush();

        long total = 0;
        long failed = 0;
        long ratePosition = 0;
        long rateStart = clock.currentTimeMillis();

        final DateRange now = DateRange.now(clock);

        try (final BufferedReader input = new BufferedReader(open(io, params.getFile()))) {
            String line;

            while ((line = input.readLine()) != null) {
                if (rateLimiter.isPresent()) {
                    rateLimiter.get().acquire();
                }

                final Series series = mapper.readValue(line, Series.class);

                if (rateLimiter.isPresent()) {
                    rateLimiter.get().acquire();
                }

                total++;

                try {
                    target.write(new WriteSuggest.Request(series, now)).get();
                } catch (Exception e) {
                    failed++;
                }

                if (total % OUTPUT_STEP == 0) {
                    if (failed > 0) {
                        io.out().print('!');
                        failed = 0;
                    } else {
                        io.out().print('#');
                    }

                    if (total % (OUTPUT_STEP * 20) == 0) {
                        long rateNow = clock.currentTimeMillis();
                        final long rate;

                        if (rateNow == rateStart) {
                            rate = -1;
                        } else {
                            rate = ((total - ratePosition) * 1000) / (rateNow - rateStart);
                        }

                        io
                            .out()
                            .println(
                                String.format(" %d (%s/s)", total, rate == -1 ? "infinite" : rate));
                        ratePosition = total;
                        rateStart = rateNow;
                    }

                    io.out().flush();
                }
            }
        }

        io.out().println();
        io.out().println("Allegedly successful writes: " + (total - failed));
        io.out().println("Allegedly failed writes: " + failed);
        io.out().flush();

        return async.resolved();
    }

    private InputStreamReader open(ShellIO io, Path file) throws IOException {
        final InputStream input = io.newInputStream(file);

        // unpack gzip.
        if (!file.getFileName().toString().endsWith(".gz")) {
            return new InputStreamReader(input, Charsets.UTF_8);
        }

        return new InputStreamReader(new GZIPInputStream(input), Charsets.UTF_8);
    }

    public static MetadataLoad setup(final CoreComponent core) {
        return DaggerMetadataLoad_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        MetadataLoad task();
    }
}
