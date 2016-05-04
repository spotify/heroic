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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.ingestion.IngestionGroup;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;
import lombok.ToString;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@TaskUsage("Write a single, or a set of events")
@TaskName("write")
public class Write implements ShellTask {
    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE =
        new TypeReference<Map<String, Object>>() {
        };

    private final IngestionManager ingestion;
    private final AsyncFramework async;
    private final ObjectMapper json;

    @Inject
    public Write(
        IngestionManager ingestion, AsyncFramework async,
        @Named("application/json") ObjectMapper json
    ) {
        this.ingestion = ingestion;
        this.async = async;
        this.json = json;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Series series;

        if (params.series == null) {
            series = Series.empty();
        } else {
            series = json.readValue(params.series, Series.class);
        }

        final IngestionGroup g = ingestion.useGroup(params.group);

        final long now = System.currentTimeMillis();

        final List<Point> points = parsePoints(params.points, now);
        final List<Event> events = parseEvents(params.events, now);

        final ImmutableList.Builder<MetricCollection> groups = ImmutableList.builder();

        io.out().println("series: " + series.toString());

        if (!points.isEmpty()) {
            int i = 0;

            io.out().println(String.format("Writing %d point(s):", points.size()));

            for (final Metric p : points) {
                io.out().println(String.format("%d: %s", i++, p));
            }

            groups.add(MetricCollection.points(points));
        }

        if (!events.isEmpty()) {
            int i = 0;

            io.out().println(String.format("Writing %d event(s):", events.size()));

            for (final Metric p : events) {
                io.out().println(String.format("%d: %s", i++, p));
            }

            groups.add(MetricCollection.events(events));
        }

        io.out().flush();

        List<AsyncFuture<Void>> writes = new ArrayList<>();

        for (final MetricCollection group : groups.build()) {
            writes.add(g
                .write(new WriteMetric(series, group))
                .directTransform(reportResult("metrics", io.out())));
        }

        return async.collectAndDiscard(writes);
    }

    private Transform<WriteResult, Void> reportResult(final String title, final PrintWriter out) {
        return (result) -> {
            synchronized (out) {
                int i = 0;

                out.println(String.format("%s: Wrote %d", title, result.getTimes().size()));

                for (final long time : result.getTimes()) {
                    out.println(String.format("  #%03d %s", i++, Tasks.formatTimeNanos(time)));
                }

                out.flush();
            }

            return null;
        };
    }

    List<Event> parseEvents(List<String> points, long now) throws IOException {
        final List<Event> output = new ArrayList<>();

        for (final String p : points) {
            final String[] parts = p.split("=");

            final long timestamp;
            final Map<String, Object> payload;

            if (parts.length == 1) {
                timestamp = now;
                payload = json.readValue(parts[0], PAYLOAD_TYPE);
            } else {
                timestamp = Tasks.parseInstant(parts[0], now);
                payload = json.readValue(parts[1], PAYLOAD_TYPE);
            }

            output.add(new Event(timestamp, payload));
        }

        return output;
    }

    List<Point> parsePoints(List<String> points, long now) {
        final List<Point> output = new ArrayList<>();

        for (final String p : points) {
            final String[] parts = p.split("=");

            final long timestamp;
            final double value;

            if (parts.length == 1) {
                timestamp = now;
                value = Double.valueOf(parts[0]);
            } else {
                timestamp = Tasks.parseInstant(parts[0], now);
                value = Double.valueOf(parts[1]);
            }

            output.add(new Point(timestamp, value));
        }

        return output;
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-s", aliases = {"--series"}, usage = "Series to fetch", metaVar = "<json>")
        private String series;

        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private String group = null;

        @Option(name = "-p", aliases = {"--point"}, usage = "Point to write",
            metaVar = "<time>=<value>")
        private List<String> points = new ArrayList<>();

        @Option(name = "-e", aliases = {"--event"}, usage = "Event to write",
            metaVar = "<time>=<payload>")
        private List<String> events = new ArrayList<>();
    }

    public static Write setup(final CoreComponent core) {
        return DaggerWrite_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        Write task();
    }
}
