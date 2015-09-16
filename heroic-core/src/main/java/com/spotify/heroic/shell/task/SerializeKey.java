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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.PrintWriter;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.ToString;

@TaskUsage("Serialize the given backend key")
@TaskName("serialize-key")
public class SerializeKey implements ShellTask {
    @Inject
    private MetricManager metrics;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final BackendKey key = mapper.readValue(params.key, BackendKeyArgument.class).toBackendKey();

        return metrics.useGroup(params.group).serializeKeyToHex(key)
                .transform(new Transform<List<String>, Void>() {
                    @Override
                    public Void transform(List<String> result) throws Exception {
                        int i = 0;

                        for (final String key : result) {
                            out.println(String.format("%d: %s", i++, key));
                        }

                        return null;
                    }
                });
    }

    @Data
    public static class BackendKeyArgument {
        private final Series series;
        private final long base;

        @JsonCreator
        public BackendKeyArgument(@JsonProperty("series") Series series, @JsonProperty("base") Long base) {
            this.series = checkNotNull(series, "series");
            this.base = checkNotNull(base, "base");
        }

        public BackendKey toBackendKey() {
            return new BackendKey(series, base);
        }
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--group", usage = "Backend group to use", metaVar = "<group>")
        private String group = null;

        @Argument(metaVar = "<json>", usage = "Key to serialize")
        private String key;
    }
}