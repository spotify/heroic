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
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import javax.inject.Inject;
import javax.inject.Named;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

@TaskUsage("Serialize the given backend key")
@TaskName("serialize-key")
public class SerializeKey implements ShellTask {
    private final MetricManager metrics;
    private final ObjectMapper mapper;

    @Inject
    public SerializeKey(MetricManager metrics, @Named("application/json") ObjectMapper mapper) {
        this.metrics = metrics;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final BackendKey backendKey =
            mapper.readValue(params.key, SerializeKeyBackendKeyArgument.class).toBackendKey();

        return metrics
            .useGroup(params.group)
            .serializeKeyToHex(backendKey)
            .directTransform(result -> {
                int i = 0;

                for (final String key : result) {
                    io.out().println(String.format("%d: %s", i++, key));
                }

                return null;
            });
    }

    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--group", usage = "Backend group to use", metaVar = "<group>")
        private String group = null;

        @Argument(metaVar = "<json>", usage = "Key to serialize")
        private String key;
    }

    public static SerializeKey setup(final CoreComponent core) {
        return DaggerSerializeKey_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        SerializeKey task();
    }
}
