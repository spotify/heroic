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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.ToString;

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.Event;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.MetricTypedGroup;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@TaskUsage("Write a single, or a set of events")
@TaskName("write")
public class Write implements ShellTask {
    private static final TypeReference<Map<String, Object>> PAYLOAD_TYPE = new TypeReference<Map<String, Object>>() {
    };

    @Inject
    private MetricManager metrics;

    @Inject
    private MetadataManager metadata;

    @Inject
    private SuggestManager suggest;

    @Inject
    private AsyncFramework async;

    @Inject
    @Named("application/json")
    private ObjectMapper json;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Series series;

        if (params.series == null) {
            series = Series.empty();
        } else {
            series = json.readValue(params.series, Series.class);
        }

        final MetricBackendGroup g = metrics.useGroup(params.group);

        final long now = System.currentTimeMillis();

        final List<Metric> points = parsePoints(params.points, now);
        final List<Metric> events = parseEvents(params.events, now);

        final ImmutableList.Builder<MetricTypedGroup> groups = ImmutableList.builder();

        out.println("series: " + series.toString());

        if (!points.isEmpty()) {
            int i = 0;

            out.println(String.format("Writing %d point(s):", points.size()));

            for (final Metric p : points) {
                out.println(String.format("%d: %s", i++, p));
            }

            groups.add(new MetricTypedGroup(MetricType.POINT, points));
        }

        if (!events.isEmpty()) {
            int i = 0;

            out.println(String.format("Writing %d event(s):", events.size()));

            for (final Metric p : events) {
                out.println(String.format("%d: %s", i++, p));
            }

            groups.add(new MetricTypedGroup(MetricType.EVENT, events));
        }

        out.flush();

        List<AsyncFuture<Void>> writes = new ArrayList<>();

        final DateRange range = DateRange.now(now);

        if (!params.noMetadata) {
            final MetadataBackend m = metadata.useGroup(params.group);
            writes.add(m.write(series, range).transform(reportResult("metadata", out)));
        }

        if (!params.noSuggest) {
            final SuggestBackend s = suggest.useGroup(params.group);
            writes.add(s.write(series, range).transform(reportResult("suggest", out)));
        }

        writes.add(g.write(new WriteMetric(series, groups.build())).transform(reportResult("metrics", out)));

        return async.collectAndDiscard(writes);
    }

    private Transform<WriteResult, Void> reportResult(final String title, final PrintWriter out) {
        return new Transform<WriteResult, Void>() {
            @Override
            public Void transform(WriteResult result) throws Exception {
                synchronized (out) {
                    int i = 0;

                    out.println(String.format("%s: Wrote %d", title, result.getTimes().size()));

                    for (final long time : result.getTimes()) {
                        out.println(String.format("  #%03d %s", i++, Tasks.formatTimeNanos(time)));
                    }

                    out.flush();
                }

                return null;
            }
        };
    }

    List<Metric> parseEvents(List<String> points, long now) throws IOException {
        final List<Metric> output = new ArrayList<>();

        for (final String p : points) {
            final String parts[] = p.split("=");

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

    List<Metric> parsePoints(List<String> points, long now) {
        final List<Metric> output = new ArrayList<>();

        for (final String p : points) {
            final String parts[] = p.split("=");

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
        @Option(name = "-s", aliases = { "--series" }, usage = "Series to fetch", metaVar = "<json>")
        private String series;

        @Option(name = "-g", aliases = { "--group" }, usage = "Backend group to use", metaVar = "<group>")
        private String group = null;

        @Option(name = "--no-metadata", usage = "Do not write metadata")
        private boolean noMetadata = false;

        @Option(name = "--no-suggest", usage = "Do not write suggestions")
        private boolean noSuggest = false;

        @Option(name = "-p", aliases = { "--point" }, usage = "Point to write", metaVar = "<time>=<value>")
        private List<String> points = new ArrayList<>();

        @Option(name = "-e", aliases = { "--event" }, usage = "Event to write", metaVar = "<time>=<payload>")
        private List<String> events = new ArrayList<>();
    }
}