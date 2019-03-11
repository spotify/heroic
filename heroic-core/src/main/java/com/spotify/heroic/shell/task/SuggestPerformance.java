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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.time.Clock;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPInputStream;
import javax.inject.Inject;
import javax.inject.Named;
import org.kohsuke.args4j.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TaskUsage("Execute a set of suggest performance tests")
@TaskName("suggest-performance")
public class SuggestPerformance implements ShellTask {
    private static final Logger log = LoggerFactory.getLogger(SuggestPerformance.class);
    private final Clock clock;
    private final SuggestManager suggest;
    private final QueryParser parser;
    private final ObjectMapper mapper;
    private final AsyncFramework async;

    @Inject
    public SuggestPerformance(
        Clock clock, SuggestManager suggest, QueryParser parser,
        @Named("application/json") ObjectMapper mapper, AsyncFramework async
    ) {
        this.clock = clock;
        this.suggest = suggest;
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

        final SuggestBackend s = suggest.useOptionalGroup(params.group);

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final List<Callable<TestResult>> tests = new ArrayList<>();

        final DateRange range = DateRange.now(clock);

        try (final InputStream input = open(io, params.file)) {
            final TestSuite suite = mapper.readValue(input, TestSuite.class);

            for (final TestCase c : suite.getTests()) {
                final Filter filter = parser.parseFilter(c.getContext());

                for (final int concurrency : suite.getConcurrency()) {
                    tests.add(setupTest(io.out(), c.getContext(), concurrency, filter, range,
                        params.getLimit(), c, s));
                }
            }
        }

        final ObjectMapper m = new ObjectMapper();

        for (final Callable<TestResult> test : tests) {
            final TestResult result = test.call();

            final TestOutput output =
                new TestOutput(result.getContext(), result.getConcurrency(), result.getErrors(),
                    result.getMismatches(), result.getMatches(), result.getCount(),
                    result.getTimes());

            io.out().println(m.writeValueAsString(output));
            io.out().flush();
        }

        return async.resolved();
    }

    private Callable<TestResult> setupTest(
        final PrintWriter out, final String context, final int concurrency, final Filter filter,
        final DateRange range, final OptionalLimit limit, final TestCase c, final SuggestBackend s
    ) {
        return () -> {
            log.info("Running context {} with concurrency {}", context, concurrency);
            final ExecutorService service = Executors.newFixedThreadPool(concurrency);
            final List<Future<TestPartialResult>> futures = new ArrayList<>();

            final AtomicInteger index = new AtomicInteger();

            final int count = c.getCount();
            final List<TestSuggestion> suggestions = c.getSuggestions();

            for (int i = 0; i < concurrency; i++) {
                futures.add(service.submit(
                    setupTestThread(out, index, count, suggestions, filter, range, limit, s)));
            }

            final List<Long> times = new ArrayList<>();
            int errors = 0;
            int mismatches = 0;
            int matches = 0;

            for (final Future<TestPartialResult> future : futures) {
                final TestPartialResult partial = future.get();
                times.addAll(partial.getTimes());
                errors += partial.getErrors();
                mismatches += partial.getMismatches();
                matches += partial.getMatches();
            }

            Collections.sort(times);

            service.shutdown();
            service.awaitTermination(10, TimeUnit.SECONDS);
            return new TestResult(context, concurrency, times, errors, mismatches, matches, count);
        };
    }

    private Callable<TestPartialResult> setupTestThread(
        final PrintWriter out, final AtomicInteger index, final int count,
        final List<TestSuggestion> suggestions, final Filter filter, final DateRange range,
        final OptionalLimit limit, final SuggestBackend s
    ) {
        return () -> {
            int i;

            final List<Long> times = new ArrayList<>();
            int errors = 0;
            int mismatches = 0;
            int matches = 0;

            while ((i = index.getAndIncrement()) < count) {
                final TestSuggestion test = suggestions.get(i % suggestions.size());
                final long start = System.nanoTime();

                final Suggestion input = test.getInput();

                final AsyncFuture<TagSuggest> future = s.tagSuggest(
                    new TagSuggest.Request(filter, range, limit, MatchOptions.builder().build(),
                        input.getOptionalKey(), input.getOptionalValue()));

                final TagSuggest result;

                try {
                    result = future.get();
                } catch (ExecutionException e) {
                    errors++;
                    continue;
                }

                final Set<Suggestion> expect = new HashSet<>(test.getExpect());

                if (result.getSuggestions().isEmpty()) {
                    log.error("no matches");
                    mismatches++;
                    continue;
                }

                for (TagSuggest.Suggestion s1 : result.getSuggestions()) {
                    expect.remove(
                        new Suggestion(s1.getKey(), s1.getValue()));

                    if (expect.isEmpty()) {
                        break;
                    }
                }

                if (!expect.isEmpty()) {
                    log.error("{} <> {}", expect, result.getSuggestions());
                }

                matches++;
                final long diff = System.nanoTime() - start;
                times.add(diff);
            }

            // put the service under load.
            return new TestPartialResult(times, errors, mismatches, matches);
        };
    }

    private InputStream open(ShellIO io, Path file) throws IOException {
        final InputStream input = io.newInputStream(file);

        // unpack gzip.
        if (!file.getFileName().toString().endsWith(".gz")) {
            return input;
        }

        return new GZIPInputStream(input);
    }

    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private Optional<String> group = Optional.empty();

        @Option(name = "-f", usage = "File to load tests from", metaVar = "<yaml>")
        private Path file = Paths.get("tests.yaml");

        @Option(name = "-l", usage = "Limit the number of results", metaVar = "<int>")
        private OptionalLimit limit = OptionalLimit.empty();

        public Path getFile() {
            return this.file;
        }

        public OptionalLimit getLimit() {
            return this.limit;
        }
    }

    public static SuggestPerformance setup(final CoreComponent core) {
        return DaggerSuggestPerformance_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        SuggestPerformance task();
    }
}
