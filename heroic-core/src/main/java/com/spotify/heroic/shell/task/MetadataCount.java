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

import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import lombok.Getter;
import lombok.ToString;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@TaskUsage("Count how much metadata matches a given query")
@TaskName("metadata-count")
public class MetadataCount implements ShellTask {
    private final MetadataManager metadata;
    private final QueryParser parser;
    private final FilterFactory filters;

    @Inject
    public MetadataCount(MetadataManager metadata, QueryParser parser, FilterFactory filters) {
        this.metadata = metadata;
        this.parser = parser;
        this.filters = filters;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        // final Filter filter = Tasks.setupFilter(filters, parser, params);
        final RangeFilter filter = Tasks.setupRangeFilter(filters, parser, params);

        return metadata.useGroup(params.group).countSeries(filter).directTransform(result -> {
            io.out().println(String.format("Found %d serie(s)", result.getCount()));
            return null;
        });
    }

    @ToString
    private static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private String group;

        @Option(name = "--limit", usage = "Limit the number of deletes (default: alot)")
        @Getter
        private int limit = Integer.MAX_VALUE;

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }

    public static MetadataCount setup(final CoreComponent core) {
        return DaggerMetadataCount_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        MetadataCount task();
    }
}
