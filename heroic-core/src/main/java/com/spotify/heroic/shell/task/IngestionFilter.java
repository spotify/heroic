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
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;
import org.kohsuke.args4j.Argument;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@TaskUsage("Configure the ingestion filter")
@TaskName("ingestion-filter")
public class IngestionFilter implements ShellTask {
    private final QueryParser parser;
    private final IngestionManager ingestion;

    @Inject
    public IngestionFilter(QueryParser parser, IngestionManager ingestion) {
        this.parser = parser;
        this.ingestion = ingestion;
    }

    static final Joiner filterJoiner = Joiner.on(" ");

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        if (params.filter.isEmpty()) {
            return ingestion.getFilter().directTransform(f -> {
                io.out().println("Current ingestion filter: " + f);
                return null;
            });
        }

        final Filter filter = parser.parseFilter(filterJoiner.join(params.filter));

        io.out().println("Updating ingestion filter to: " + filter);
        return ingestion.setFilter(filter);
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Argument(metaVar = "<filter>", usage = "Filter to use")
        private List<String> filter = new ArrayList<>();
    }

    public static IngestionFilter setup(final CoreComponent core) {
        return DaggerIngestionFilter_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        IngestionFilter task();
    }
}
