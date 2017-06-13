/*
 * Copyright (c) 2017 Spotify AB.
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
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.time.Clock;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.ToString;
import org.kohsuke.args4j.Option;

@TaskUsage("Write metadata")
@TaskName("metadata-write")
public class MetadataWrite implements ShellTask {
    private final Clock clock;
    private final MetadataManager metadataManager;
    private final ObjectMapper json;

    @Inject
    public MetadataWrite(
        Clock clock, MetadataManager metadataManager, @Named("application/json") ObjectMapper json
    ) {
        this.clock = clock;
        this.metadataManager = metadataManager;
        this.json = json;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Series series = json.readValue(params.series, Series.class);

        return metadataManager
            .useGroup(params.group)
            .write(new WriteMetadata.Request(series, DateRange.now(clock)))
            .directTransform(v -> null);
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-s", aliases = {"--series"}, usage = "Series to fetch", metaVar = "<json>",
            required = true)
        private String series;

        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private String group = null;
    }

    public static MetadataWrite setup(final CoreComponent core) {
        return DaggerMetadataWrite_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        MetadataWrite task();
    }
}
