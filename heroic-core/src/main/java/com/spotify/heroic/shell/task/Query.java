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

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.metric.MetricTypedGroup;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;
import lombok.ToString;

@TaskUsage("Execute a query")
@TaskName("query")
public class Query implements ShellTask {
    @Inject
    private QueryManager query;

    @Inject
    @Named("application/json")
    private ObjectMapper mapper;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final PrintWriter out, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final String queryString = params.query.stream().collect(Collectors.joining(" "));

        final AsyncFuture<QueryResult> result = query.useGroup(params.group).query(
                query.newQuery().queryString(queryString).build());

        final ObjectMapper indent = mapper.copy();
        indent.configure(SerializationFeature.INDENT_OUTPUT, true);

        return result.transform(new Transform<QueryResult, Void>() {
            @Override
            public Void transform(QueryResult result) throws Exception {
                for (final RequestError e : result.getErrors()) {
                    out.println(String.format("ERR: %s", e.toString()));
                }

                for (final ShardedResultGroup resultGroup : result.getGroups()) {
                    final MetricTypedGroup group = resultGroup.getGroup();

                    out.println(String.format("%s: %s %s", group.getType(), resultGroup.getShard(),
                            resultGroup.getTags()));
                    out.println(indent.writeValueAsString(group.getData()));
                    out.flush();
                }

                return null;
            }
        });
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-g", aliases = { "--group" }, usage = "Backend group to use", metaVar = "<group>")
        private String group = null;

        @Argument(metaVar = "<query>")
        private List<String> query = new ArrayList<>();
    }
}