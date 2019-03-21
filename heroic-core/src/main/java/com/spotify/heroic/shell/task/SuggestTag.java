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
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.shell.task.parameters.SuggestTagParameters;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.TagSuggest;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import javax.inject.Inject;
import javax.inject.Named;

@TaskUsage("Get tag suggestions")
@TaskName("suggest-tag")
public class SuggestTag implements ShellTask {
    private final SuggestManager suggest;
    private final QueryParser parser;
    private final ObjectMapper mapper;

    @Inject
    public SuggestTag(
        SuggestManager suggest, QueryParser parser, @Named("application/json") ObjectMapper mapper
    ) {
        this.suggest = suggest;
        this.parser = parser;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new SuggestTagParameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final SuggestTagParameters params = (SuggestTagParameters) base;

        final Filter filter = Tasks.setupFilter(parser, params);

        final MatchOptions fuzzyOptions = MatchOptions.builder().build();

        return suggest
            .useOptionalGroup(params.getGroup())
            .tagSuggest(
                new TagSuggest.Request(filter, params.getRange(), params.getLimit(), fuzzyOptions,
                    params.getKey(), params.getValue()))
            .directTransform(result -> {
                int i = 0;

                for (final TagSuggest.Suggestion suggestion : result.getSuggestions()) {
                    io.out().println(String.format("%s: %s", i++, suggestion));
                }

                return null;
            });
    }

    public static SuggestTag setup(final CoreComponent core) {
        return DaggerSuggestTag_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        SuggestTag task();
    }
}
