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
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.TagValuesSuggest;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import lombok.Getter;
import lombok.ToString;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@TaskUsage("Get a list of value suggestions for a given key")
@TaskName("suggest-tag-values")
public class SuggestTagValues implements ShellTask {
    private final SuggestManager suggest;
    private final QueryParser parser;

    @Inject
    public SuggestTagValues(SuggestManager suggest, QueryParser parser) {
        this.suggest = suggest;
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

        return suggest
            .useOptionalGroup(params.group)
            .tagValuesSuggest(
                new TagValuesSuggest.Request(filter, params.getRange(), params.getLimit(),
                    params.groupLimit, params.exclude))
            .directTransform(result -> {
                int i = 0;

                for (final TagValuesSuggest.Suggestion suggestion : result.getSuggestions()) {
                    io.out().println(String.format("%s: %s", i++, suggestion));
                }

                return null;
            });
    }

    @ToString
    static class Parameters extends Tasks.QueryParamsBase {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private Optional<String> group = Optional.empty();

        @Option(name = "-e", aliases = {"--exclude"}, usage = "Exclude the given tags")
        private List<String> exclude = new ArrayList<>();

        @Option(name = "--group-limit", usage = "Maximum cardinality to pull")
        private OptionalLimit groupLimit = OptionalLimit.empty();

        @Option(name = "--limit", aliases = {"--limit"},
            usage = "Limit the number of printed entries")
        @Getter
        private OptionalLimit limit = OptionalLimit.empty();

        @Argument
        @Getter
        private List<String> query = new ArrayList<String>();
    }

    public static SuggestTagValues setup(final CoreComponent core) {
        return DaggerSuggestTagValues_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        SuggestTagValues task();
    }
}
