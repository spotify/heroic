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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.RateLimiter;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Getter;
import lombok.ToString;

@TaskUsage("Load metadata from a file")
@TaskName("metadata-load")
public class MetadataLoad implements ShellTask {
    protected static final long OUTPUT_STEP = 1000;

    @Inject
    private AsyncFramework async;

    @Inject
    private SuggestManager suggest;

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

        final SuggestBackend target = suggest.useGroup(params.target);

        final Optional<RateLimiter> rateLimiter = params.rate <= 0 ? Optional.<RateLimiter> absent()
                : Optional.of(RateLimiter.create(params.rate));

        io.out().println("Loading suggest data:");
        io.out().println("  from (file): " + params.file);
        io.out().println("  to  (suggest): " + target);
        io.out().println("  rate-limit:" + (rateLimiter.isPresent() ? params.rate : "disabled"));
        io.out().flush();

        long total = 0;
        long failed = 0;
        long ratePosition = 0;
        long rateStart = System.currentTimeMillis();

        final DateRange now = DateRange.now();

        try (final BufferedReader input = new BufferedReader(open(io, params.file))) {
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
                    target.write(series, now).get();
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
                        long rateNow = System.currentTimeMillis();
                        final long rate;

                        if (rateNow == rateStart) {
                            rate = -1;
                        } else {
                            rate = ((total - ratePosition) * 1000) / (rateNow - rateStart);
                        }

                        io.out().println(
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

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-t", aliases = {"--target"}, usage = "Backend group to migrate to",
                metaVar = "<metadata-group>")
        private String target;

        @Option(name = "-f", usage = "File to load from", required = true)
        @Getter
        private Path file = Paths.get("series");

        @Option(name = "-r", usage = "Rate-limit for writing to ES. 0 means disabled")
        @Getter
        private int rate = 0;
    }
}
