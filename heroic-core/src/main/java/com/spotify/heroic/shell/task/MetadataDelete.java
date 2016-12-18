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

import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskParametersModel;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.LazyTransform;
import lombok.Getter;
import lombok.ToString;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@TaskUsage("Delete metadata matching the given query")
@TaskName("metadata-delete")
@TaskParametersModel(MetadataDelete.Parameters.class)
public class MetadataDelete implements ShellTask {
    private final AsyncFramework async;
    private final MetadataManager metadata;
    private final QueryParser parser;

    @Inject
    public MetadataDelete(
        AsyncFramework async, MetadataManager metadata, QueryParser parser
    ) {
        this.async = async;
        this.metadata = metadata;
        this.parser = parser;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Filter filter = Tasks.setupFilter(parser, params);

        final MetadataBackend group = metadata.useOptionalGroup(params.group);

        return group
            .countSeries(new CountSeries.Request(filter, params.getRange(), params.getLimit()))
            .lazyTransform((LazyTransform<CountSeries, Void>) c -> {
                io.out().println(String.format("Deleteing %d entrie(s)", c.getCount()));

                if (!params.ok) {
                    io.out().println("Deletion stopped, use --ok to proceed");
                    return async.resolved(null);
                }

                return group
                    .deleteSeries(
                        new DeleteSeries.Request(filter, params.getRange(), params.getLimit()))
                    .directTransform(r -> null);
            });
    }

    @ToString
    static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private Optional<String> group = Optional.empty();

        @Option(name = "--ok", usage = "Verify that you actually want to run")
        private boolean ok = false;

        @Option(name = "--limit", usage = "Limit the number of deletes (default: alot)",
            metaVar = "<number>")
        @Getter
        private OptionalLimit limit = OptionalLimit.empty();

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }

    public static MetadataDelete setup(final CoreComponent core) {
        return DaggerMetadataDelete_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        MetadataDelete task();
    }
}
