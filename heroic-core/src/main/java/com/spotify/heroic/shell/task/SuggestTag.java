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
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.TagSuggest;
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

@TaskUsage("Get tag suggestions")
@TaskName("suggest-tag")
public class SuggestTag implements ShellTask {
    private final SuggestManager suggest;
    private final FilterFactory filters;
    private final QueryParser parser;
    private final ObjectMapper mapper;

    @Inject
    public SuggestTag(
        SuggestManager suggest, FilterFactory filters, QueryParser parser,
        @Named("application/json") ObjectMapper mapper
    ) {
        this.suggest = suggest;
        this.filters = filters;
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

        final RangeFilter filter = Tasks.setupRangeFilter(filters, parser, params);

        final MatchOptions fuzzyOptions = MatchOptions.builder().build();

        return suggest
            .useGroup(params.group)
            .tagSuggest(filter, fuzzyOptions, Optional.ofNullable(params.key),
                Optional.ofNullable(params.value))
            .directTransform(result -> {
                int i = 0;

                for (final TagSuggest.Suggestion suggestion : result.getSuggestions()) {
                    io.out().println(String.format("%s: %s", i++, suggestion));
                }

                return null;
            });
    }

    @ToString
    private static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private String group;

        @Option(name = "-k", aliases = {"--key"}, usage = "Provide key context for suggestion")
        private String key = null;

        @Option(name = "-v", aliases = {"--value"}, usage = "Provide value context for suggestion")
        private String value = null;

        @Option(name = "--limit", aliases = {"--limit"},
            usage = "Limit the number of printed entries")
        @Getter
        private int limit = 10;

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }

    public static SuggestTag setup(final CoreComponent core) {
        return DaggerSuggestTag_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        SuggestTag task();
    }
}
