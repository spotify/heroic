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
import java.util.List;

import lombok.Data;
import lombok.ToString;

import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.model.BackendKey;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.shell.CoreBridge;
import com.spotify.heroic.shell.CoreBridge.BaseParams;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;

@Usage("List available metric keys for all backends")
public class Keys implements CoreBridge.Task {
    public static void main(String argv[]) throws Exception {
        CoreBridge.standalone(argv, Keys.class);
    }

    @Inject
    private MetricManager metrics;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Override
    public BaseParams params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, BaseParams base) throws Exception {
        final Parameters params = (Parameters) base;

        final Series series;

        if (params.series != null) {
            series = mapper.readValue(params.series, Series.class);
        } else {
            series = null;
        }

        final BackendKey start;

        if (params.start != null) {
            start = mapper.readValue(params.start, BackendKeyArgument.class).toBackendKey();
        } else {
            start = seriesStart(series);
        }

        final BackendKey end;

        if (params.end != null) {
            end = mapper.readValue(params.end, BackendKeyArgument.class).toBackendKey();
        } else {
            end = seriesEnd(series);
        }

        final int limit = Math.max(1, Math.min(10000, params.limit));

        return metrics.useGroup(params.group).keys(start, end, limit)
                .transform(new Transform<List<BackendKey>, Void>() {
                    @Override
                    public Void transform(List<BackendKey> result) throws Exception {
                        int i = 0;

                        for (final BackendKey key : result)
                            out.println(String.format("#%03d: %s", i++, mapper.writeValueAsString(key)));

                        return null;
                    }
                });
    }

    private BackendKey seriesEnd(Series series) {
        if (series == null)
            return null;

        return new BackendKey(series, 0xffffffffffffffffl);
    }

    private BackendKey seriesStart(Series series) {
        if (series == null)
            return null;

        return new BackendKey(series, 0);
    }

    @Data
    public static class BackendKeyArgument {
        private final Series series;
        private final long base;

        @JsonCreator
        public static BackendKeyArgument create(@JsonProperty("series") Series series, @JsonProperty("base") Long base) {
            if (series == null)
                throw new IllegalArgumentException("series must be specified");

            if (base == null)
                throw new IllegalArgumentException("base must be specified");

            return new BackendKeyArgument(series, base);
        }

        public BackendKey toBackendKey() {
            return new BackendKey(series, base);
        }
    }

    @ToString
    private static class Parameters implements CoreBridge.BaseParams {
        @Option(name = "-c", aliases = { "--config" }, usage = "Path to configuration (only used in standalone)", metaVar = "<config>")
        private String config;

        @Option(name = "--series", usage = "Series to list", metaVar = "<json>")
        private String series;

        @Option(name = "--start", usage = "First key to list (overrides start value from --series)", metaVar = "<json>")
        private String start;

        @Option(name = "--end", usage = "Last key to list (overrides end value from --series)", metaVar = "<json>")
        private String end;

        @Option(name = "--limit", usage = "Maximum number of keys to list", metaVar = "<int>")
        private int limit = 10;

        @Option(name = "--group", usage = "Backend group to use", metaVar = "<group>")
        private String group = null;

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
}