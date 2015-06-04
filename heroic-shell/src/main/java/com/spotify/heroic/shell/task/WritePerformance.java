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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.Data;
import lombok.ToString;

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.model.FetchData;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.shell.CoreBridge;
import com.spotify.heroic.shell.CoreBridge.BaseParams;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.StreamCollector;
import eu.toolchain.async.Transform;

@Usage("Perform performance testing")
public class WritePerformance implements CoreBridge.Task {
    public static void main(String argv[]) throws Exception {
        CoreBridge.standalone(argv, WritePerformance.class);
    }

    @Inject
    private MetricManager metrics;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Inject
    private AsyncFramework async;

    @Override
    public BaseParams params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, BaseParams base) throws Exception {
        final Parameters params = (Parameters) base;

        final Date now = new Date();

        final List<Series> series = generateSeries(params.series);

        final long start = now.getTime() - TimeUnit.MILLISECONDS.convert(params.history, TimeUnit.SECONDS);
        final long end = now.getTime();
        final DateRange range = new DateRange(start, end);

        final MetricBackendGroup readGroup = metrics.useGroup(params.from);
        final MetricBackend writeGroup = metrics.useGroup(params.to);

        final List<AsyncFuture<WriteMetric>> reads = new ArrayList<>();

        for (final Series s : series) {
            reads.add(readGroup.fetch(DataPoint.class, s, range).transform(
                    new Transform<FetchData<DataPoint>, WriteMetric>() {
                        @Override
                        public WriteMetric transform(FetchData<DataPoint> result) throws Exception {
                            return new WriteMetric(s, result.getData());
                        }
                    }));
        }

        return async.collect(reads).transform(new Transform<Collection<WriteMetric>, Void>() {
            @Override
            public Void transform(Collection<WriteMetric> input) throws Exception {
                out.println(String.format("Read %d series(s), performing writes...", series.size()));
                out.flush();

                final long start = System.currentTimeMillis();

                int totalWrites = 0;

                for (final WriteMetric w : input) {
                    totalWrites += (w.getData().size() * params.writes);
                }

                final List<AsyncFuture<Times>> writes = buildWrites(writeGroup, input, params, start);
                final AsyncFuture<CollectedTimes> results = collectWrites(out, writes);

                final CollectedTimes times = results.get();
                final double totalRuntime = (System.currentTimeMillis() - start) / 1000.0;

                out.println(String.format("Failed: %d write(s)", times.errors));
                out.println(String.format("Time: %.2fs", totalRuntime));
                out.println(String.format("Write/s: %.2f", totalWrites / totalRuntime));
                out.println();

                printHistogram("Overall", out, times.runTimes, TimeUnit.MILLISECONDS);
                out.println();

                printHistogram("Execution Time", out, times.executionTimes, TimeUnit.NANOSECONDS);

                out.flush();
                return null;
            }
        });
    }

    private void printHistogram(String title, final PrintWriter out, final List<Long> times, TimeUnit unit) {
        if (times.isEmpty()) {
            out.println(String.format("%s: (no samples)", title));
            return;
        }

        Collections.sort(times);

        final long avg = average(times);
        final long q10 = times.get((int) (0.1 * times.size()));
        final long q50 = times.get((int) (0.5 * times.size()));
        final long q75 = times.get((int) (0.75 * times.size()));
        final long q90 = times.get((int) (0.90 * times.size()));
        final long q99 = times.get((int) (0.99 * times.size()));

        out.println(String.format("%s:", title));
        out.println(String.format(" total: %d write(s)", times.size()));
        out.println(String.format("   avg: %d ms", TimeUnit.MILLISECONDS.convert(avg, unit)));
        out.println(String.format("  10th: %d ms", TimeUnit.MILLISECONDS.convert(q10, unit)));
        out.println(String.format("  50th: %d ms", TimeUnit.MILLISECONDS.convert(q50, unit)));
        out.println(String.format("  75th: %d ms", TimeUnit.MILLISECONDS.convert(q75, unit)));
        out.println(String.format("  90th: %d ms", TimeUnit.MILLISECONDS.convert(q90, unit)));
        out.println(String.format("  99th: %d ms", TimeUnit.MILLISECONDS.convert(q99, unit)));
    }

    private List<AsyncFuture<Times>> buildWrites(MetricBackend writeGroup, Collection<WriteMetric> input,
            final Parameters params, final long start) {
        final List<AsyncFuture<Times>> writes = new ArrayList<>();

        if (params.batch) {
            for (int i = 0; i < params.writes; i++) {
                writes.add(writeGroup.write(input).transform(new Transform<WriteResult, Times>() {
                    @Override
                    public Times transform(WriteResult result) throws Exception {
                        final long runtime = System.currentTimeMillis() - start;
                        return new Times(result.getTimes(), runtime);
                    }
                }));
            }

            return writes;
        }

        for (int i = 0; i < params.writes; i++) {
            for (final WriteMetric w : input) {
                writes.add(writeGroup.write(w).transform(new Transform<WriteResult, Times>() {
                    @Override
                    public Times transform(WriteResult result) throws Exception {
                        final long runtime = System.currentTimeMillis() - start;
                        return new Times(result.getTimes(), runtime);
                    }
                }));
            }
        }

        return writes;
    }

    private AsyncFuture<CollectedTimes> collectWrites(final PrintWriter out, Collection<AsyncFuture<Times>> writes) {
        final int div = writes.size() / 40;
        final boolean mod = writes.size() % div == 0;

        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger count = new AtomicInteger();

        final AsyncFuture<CollectedTimes> results = async.collect(writes, new StreamCollector<Times, CollectedTimes>() {
            final ConcurrentLinkedQueue<Long> runtimes = new ConcurrentLinkedQueue<>();
            final ConcurrentLinkedQueue<Long> executionTimes = new ConcurrentLinkedQueue<>();

            @Override
            public void resolved(Times result) throws Exception {
                runtimes.add(result.getRuntime());
                executionTimes.addAll(result.getExecutionTimes());
                check();
            }

            @Override
            public void failed(Throwable cause) throws Exception {
                errors.incrementAndGet();
                check();
            }

            @Override
            public void cancelled() throws Exception {
                errors.incrementAndGet();
                check();
            }

            private void check() {
                if (count.incrementAndGet() % div == 0)
                    dot();
            }

            private void dot() {
                out.print(errors.getAndSet(0) > 0 ? '!' : '.');
                out.flush();
            }

            @Override
            public CollectedTimes end(int resolved, int failed, int cancelled) throws Exception {
                if (!mod)
                    dot();

                out.println();
                out.flush();

                final List<Long> runtimes = new ArrayList<Long>(this.runtimes);
                final List<Long> executionTimes = new ArrayList<>(this.executionTimes);
                return new CollectedTimes(runtimes, executionTimes, errors.get());
            }
        });

        return results;
    }

    private List<Series> generateSeries(int count) {
        final List<Series> series = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            series.add(new Series("generated",
                    ImmutableMap.of("type", "generated", "generated-id", Integer.toString(i))));
        }

        return series;
    }

    private long average(List<Long> times) {
        long total = 0;

        for (final long v : times)
            total += v;

        return total / times.size();
    }

    @ToString
    public static class Parameters implements CoreBridge.BaseParams {
        @Option(name = "-c", aliases = { "--config" }, usage = "Path to configuration (only used in standalone)", metaVar = "<config>")
        private String config;

        @Option(name = "--limit", usage = "Maximum number of datapoints to fetch (default: 1000000)", metaVar = "<int>")
        private int limit = 1000000;

        @Option(name = "--series", required = true, usage = "Number of series to fetch", metaVar = "<number>")
        private int series;

        @Option(name = "--from", required = true, usage = "Group to read data from", metaVar = "<group>")
        private String from;

        @Option(name = "--to", required = true, usage = "Group to write data to", metaVar = "<long>")
        private String to;

        @Option(name = "--history", usage = "Seconds of data to copy (default: 3600)", metaVar = "<number>")
        private long history = 3600;

        @Option(name = "--writes", usage = "How many writes to perform (default: 1000)", metaVar = "<number>")
        private int writes = 1000;

        @Option(name = "-B", aliases = { "--batch" }, usage = "Write using batch API")
        private boolean batch = false;

        @Option(name = "-h", aliases = { "--help" }, help = true, usage = "Display help")
        private boolean help;

        @Override
        public String config() {
            return config;
        }

        @Override
        public boolean help() {
            return help;
        }
    }

    @Data
    private static final class CollectedTimes {
        private final List<Long> runTimes;
        private final List<Long> executionTimes;
        private final int errors;
    }

    @Data
    private static final class Times {
        private final List<Long> executionTimes;
        private final long runtime;
    }
}
