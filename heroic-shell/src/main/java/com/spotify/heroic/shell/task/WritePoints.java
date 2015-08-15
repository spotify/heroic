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
import java.util.List;

import lombok.ToString;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.HeroicShell;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.Point;
import com.spotify.heroic.metric.MetricTypeGroup;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.shell.AbstractShellTask;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellTaskParams;
import com.spotify.heroic.shell.ShellTaskUsage;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@ShellTaskUsage("Write a single, or a set of points")
public class WritePoints extends AbstractShellTask {
    public static void main(String argv[]) throws Exception {
        HeroicShell.standalone(argv, WritePoints.class);
    }

    @Inject
    private MetricManager metrics;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Inject
    private AsyncFramework async;

    @Override
    public ShellTaskParams params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, final ShellTaskParams base) throws Exception {
        final Parameters params = (Parameters) base;

        final Series series = mapper.readValue(params.series, Series.class);

        final MetricBackendGroup g = metrics.useGroup(params.group);

        final long now = System.currentTimeMillis();
        final List<Metric> points = parsePoints(params.points, now);

        int i = 0;

        out.println("series: " + series.toString());
        out.println("points:");

        for (final Metric p : points) {
            out.println(String.format("%d: %s", i++, p));
        }

        out.flush();

        final List<MetricTypeGroup> data = ImmutableList.of(new MetricTypeGroup(MetricType.POINT, points));

        return g.write(new WriteMetric(series, data)).transform(new Transform<WriteResult, Void>() {
            @Override
            public Void transform(WriteResult result) throws Exception {
                int i = 0;

                for (final long time : result.getTimes()) {
                    out.println(String.format("%d: %dns", i++, time));
                }

                return null;
            }
        });
    }

    private List<Metric> parsePoints(List<String> points, long now) {
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
        @Option(name = "--series", required = true, usage = "Series to fetch", metaVar = "<json>")
        private String series;

        @Option(name = "-g", aliases = { "--group" }, usage = "Backend group to use", metaVar = "<group>")
        private String group = null;

        @Argument
        private List<String> points = new ArrayList<>();
    }
}