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
import com.google.common.base.Joiner;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@TaskUsage("Parse a given expression as a query and print their structure")
@TaskName("stringify-query")
public class StringifyQuery implements ShellTask {
    private final QueryParser parser;
    private final AsyncFramework async;
    private final ObjectMapper mapper;

    @Inject
    public StringifyQuery(
        QueryParser parser, AsyncFramework async, @Named("application/json") ObjectMapper mapper
    ) {
        this.parser = parser;
        this.async = async;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;
        final com.spotify.heroic.Query q =
            mapper.readValue(Joiner.on(" ").join(params.query), com.spotify.heroic.Query.class);

        io.out().println(parser.stringifyQuery(q, Optional.of(params.indent)));

        return async.resolved();
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-i", aliases = {"--indent"}, usage = "Indent output",
            metaVar = "<indent>")
        public int indent = 2;

        @Argument(usage = "Query to parse")
        private List<String> query = new ArrayList<>();
    }

    public static StringifyQuery setup(final CoreComponent core) {
        return DaggerStringifyQuery_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        StringifyQuery task();
    }
}
