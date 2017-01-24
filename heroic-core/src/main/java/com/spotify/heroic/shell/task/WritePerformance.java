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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;

import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.FetchQuotaWatcher;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.StreamCollector;

import org.kohsuke.args4j.Option;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import dagger.Component;
import lombok.Data;
import lombok.ToString;

@TaskUsage("Perform performance testing")
@TaskName("write-performance")
public class WritePerformance implements ShellTask {
    private final MetricManager metrics;
    private final AsyncFramework async;

    @Inject
    public WritePerformance(MetricManager metrics, AsyncFramework async) {
        this.metrics = metrics;
        this.async = async;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    final Joiner joiner = Joiner.on(" ");

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Date now = new Date();

        final List<Series> series = generateSeries(params.series);

        final long startRange =
            now.getTime() - TimeUnit.MILLISECONDS.convert(params.history, TimeUnit.SECONDS);
        final long endRange = now.getTime();
        final DateRange range = new DateRange(startRange, endRange);

        final MetricBackendGroup readGroup = metrics.useGroup(params.from);
        final List<MetricBackend> targets = resolveTargets(params.targets);

        final List<AsyncFuture<FetchData.Result>> reads = new ArrayList<>();
        final List<WriteMetric.Request> writeRequests =
            Collections.synchronizedList(new ArrayList<>());

        for (final Series s : series) {
            reads.add(readGroup.fetch(
                new FetchData.Request(MetricType.POINT, s, range, QueryOptions.defaults()),
                FetchQuotaWatcher.NO_QUOTA,
                mc -> writeRequests.add(new WriteMetric.Request(s, mc))));
        }

        return async.collect(reads).lazyTransform(input -> {
                final long warmupStart = System.currentTimeMillis();

                final List<CollectedTimes> warmup = new ArrayList<>();

                for (int i = 0; i < params.warmup; i++) {
                    io.out().println(String.format("Warmup step %d/%d", (i + 1), params.warmup));

                    final List<Callable<AsyncFuture<Times>>> writes =
                        buildWrites(targets, writeRequests, params, warmupStart);

                    warmup.add(collectWrites(io.out(), writes, params.parallelism).get());
                    // try to trick the JVM not to optimize out any steps
                    warmup.clear();
                }

                int totalWrites = 0;

                for (final WriteMetric.Request w : writeRequests) {
                    totalWrites += (w.getData().size() * params.writes);
                }

                final List<CollectedTimes> all = new ArrayList<>();
                final List<Double> writesPerSecond = new ArrayList<>();
                final List<Double> runtimes = new ArrayList<>();

                for (int i = 0; i < params.loop; i++) {
                    io.out().println(String.format("Running step %d/%d", (i + 1), params.loop));

                    final long start = System.currentTimeMillis();

                    final List<Callable<AsyncFuture<Times>>> writes =
                        buildWrites(targets, writeRequests, params, start);
                    all.add(collectWrites(io.out(), writes, params.parallelism).get());

                    final double totalRuntime = (System.currentTimeMillis() - start) / 1000.0;

                    writesPerSecond.add(totalWrites / totalRuntime);
                    runtimes.add(totalRuntime);
                }

                final CollectedTimes times = merge(all);

                io.out().println(String.format("Failed: %d write(s)", times.errors));
                io.out().println(String.format("Times: " + convertList(runtimes)));
                io.out().println(String.format("Write/s: " + convertList(writesPerSecond)));
                io.out().println();

                printHistogram("Batch Times", io.out(), times.runTimes, TimeUnit.MILLISECONDS);
                io.out().println();

                printHistogram("Individual Times", io.out(), times.executionTimes,
                    TimeUnit.NANOSECONDS);
                io.out().flush();

                return async.resolved();
            });
    }

    private String convertList(final List<Double> values) {
        return joiner.join(values.stream().map(v -> String.format("%.2f", v)).iterator());
    }

    private CollectedTimes merge(List<CollectedTimes> all) {
        final List<Long> runTimes = new ArrayList<>();
        final List<Long> executionTimes = new ArrayList<>();
        int errors = 0;

        for (final CollectedTimes time : all) {
            runTimes.addAll(time.runTimes);
            executionTimes.addAll(time.executionTimes);
            errors += time.errors;
        }

        return new CollectedTimes(runTimes, executionTimes, errors);
    }

    private List<MetricBackend> resolveTargets(List<String> targets) {
        if (targets.isEmpty()) {
            throw new IllegalArgumentException("'targets' is empty, add some with --target");
        }

        final List<MetricBackend> backends = new ArrayList<>();

        for (final String target : targets) {
            backends.add(metrics.useGroup(target));
        }

        return backends;
    }

    private void printHistogram(
        String title, final PrintWriter out, final List<Long> times, TimeUnit unit
    ) {
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

    private List<Callable<AsyncFuture<Times>>> buildWrites(
        List<MetricBackend> targets, List<WriteMetric.Request> input, final Parameters params,
        final long start
    ) {
        final List<Callable<AsyncFuture<Times>>> writes = new ArrayList<>();

        int request = 0;

        for (int i = 0; i < params.writes; i++) {
            for (final WriteMetric.Request w : input) {
                final MetricBackend target = targets.get(request++ % targets.size());

                writes.add(() -> target.write(w).directTransform(result -> {
                    final long runtime = System.currentTimeMillis() - start;
                    return new Times(result.getTimes(), runtime);
                }));
            }
        }

        return writes;
    }

    private AsyncFuture<CollectedTimes> collectWrites(
        final PrintWriter out, Collection<Callable<AsyncFuture<Times>>> writes, int parallelism
    ) {
        final int div = Math.max(writes.size() / 40, 1);
        final boolean mod = writes.size() % div == 0;

        final AtomicInteger errors = new AtomicInteger();
        final AtomicInteger count = new AtomicInteger();

        final AsyncFuture<CollectedTimes> results =
            async.eventuallyCollect(writes, new StreamCollector<Times, CollectedTimes>() {
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
                    cause.printStackTrace(out);
                    errors.incrementAndGet();
                    check();
                }

                @Override
                public void cancelled() throws Exception {
                    errors.incrementAndGet();
                    check();
                }

                private void check() {
                    if (count.incrementAndGet() % div == 0) {
                        dot();
                    }
                }

                private void dot() {
                    out.print(errors.getAndSet(0) > 0 ? '!' : '.');
                    out.flush();
                }

                @Override
                public CollectedTimes end(int resolved, int failed, int cancelled)
                    throws Exception {
                    if (!mod) {
                        dot();
                    }

                    out.println();
                    out.flush();

                    final List<Long> runtimes = new ArrayList<Long>(this.runtimes);
                    final List<Long> executionTimes = new ArrayList<>(this.executionTimes);
                    return new CollectedTimes(runtimes, executionTimes, errors.get());
                }
            }, parallelism);

        return results;
    }

    private List<Series> generateSeries(int count) {
        final List<Series> series = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            series.add(Series.of("generated",
                ImmutableMap.of("type", "generated", "generated-id", Integer.toString(i))));
        }

        return series;
    }

    private long average(List<Long> times) {
        long total = 0;

        for (final long v : times) {
            total += v;
        }

        return total / times.size();
    }

    @ToString
    public static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--limit",
            usage = "Maximum number of datapoints to fetch (default: 1000000)",
            metaVar = "<int>")
        private int limit = 1000000;

        @Option(name = "--series", required = true, usage = "Number of different series to write",
            metaVar = "<number>")
        private int series = 10;

        @Option(name = "--from", required = true, usage = "Group to read data from",
            metaVar = "<group>")
        private String from;

        @Option(name = "--target", usage = "Group to write data to", metaVar = "<backend>")
        private List<String> targets = new ArrayList<>();

        @Option(name = "--history", usage = "Seconds of data to copy (default: 3600)",
            metaVar = "<number>")
        private long history = 3600;

        @Option(name = "--writes", usage = "How many writes to perform (default: 1000)",
            metaVar = "<number>")
        private int writes = 10;

        @Option(name = "--parallelism",
            usage = "The number of requests to send in parallel (default: 100)",
            metaVar = "<number>")
        private int parallelism = 100;

        @Option(name = "--warmup", usage = "Loop the test several times to warm up",
            metaVar = "<number>")
        private int warmup = 4;

        @Option(name = "--loop", usage = "Loop the test several times", metaVar = "<number>")
        private int loop = 8;
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

    public static WritePerformance setup(final CoreComponent core) {
        return DaggerWritePerformance_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        WritePerformance task();
    }
}
