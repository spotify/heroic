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

import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.shell.task.parameters.SuggestTagValueParameters;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.TagValueSuggest;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import javax.inject.Inject;

@TaskUsage("Get value suggestions for a given key")
@TaskName("suggest-tag-value")
public class SuggestTagValue implements ShellTask {
    private final SuggestManager suggest;
    private final QueryParser parser;

    @Inject
    public SuggestTagValue(SuggestManager suggest, QueryParser parser) {
        this.suggest = suggest;
        this.parser = parser;
    }

    @Override
    public TaskParameters params() {
        return new SuggestTagValueParameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final SuggestTagValueParameters params = (SuggestTagValueParameters) base;

        final Filter filter = Tasks.setupFilter(parser, params);

        return suggest
            .useOptionalGroup(params.getGroup())
            .tagValueSuggest(
                new TagValueSuggest.Request(filter, params.getRange(), params.getLimit(),
                    params.getKey()))
            .directTransform(result -> {
                int i = 0;

                for (final String value : result.getValues()) {
                    io.out().println(String.format("%s: %s", i++, value));
                }

                return null;
            });
    }

    public static SuggestTagValue setup(final CoreComponent core) {
        return DaggerSuggestTagValue_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        SuggestTagValue task();
    }
}
