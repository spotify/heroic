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
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskParametersModel;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import lombok.Getter;
import lombok.ToString;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@TaskUsage("Fetch series matching the given query")
@TaskName("metadata-fetch")
@TaskParametersModel(MetadataFetch.Parameters.class)
public class MetadataFetch implements ShellTask {
    private final MetadataManager metadata;
    private final QueryParser parser;
    private final ObjectMapper mapper;

    @Inject
    public MetadataFetch(
        MetadataManager metadata, QueryParser parser, @Named("application/json") ObjectMapper mapper
    ) {
        this.metadata = metadata;
        this.parser = parser;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Filter filter = Tasks.setupFilter(parser, params);

        return metadata
            .useOptionalGroup(params.group)
            .findSeries(new FindSeries.Request(filter, params.getRange(), params.getLimit()))
            .directTransform(result -> {
                int i = 0;

                for (final Series series : result.getSeries()) {
                    io
                        .out()
                        .println(String.format("%s: %s", i++, mapper.writeValueAsString(series)));
                }

                return null;
            });
    }

    @ToString
    static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private Optional<String> group = Optional.empty();

        @Option(name = "--limit", usage = "Limit the number of printed entries",
            metaVar = "<number>")
        @Getter
        private OptionalLimit limit = OptionalLimit.empty();

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }

    public static MetadataFetch setup(final CoreComponent core) {
        return DaggerMetadataFetch_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        MetadataFetch task();
    }
}
