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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.spotify.heroic.QueryBuilder;
import com.spotify.heroic.QueryDateRange;
import com.spotify.heroic.QueryManager;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.metric.Tracing;
import com.spotify.heroic.querylogging.QueryContext;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import dagger.Component;
import eu.toolchain.async.AsyncFuture;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.ToString;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

@TaskUsage("Execute a query")
@TaskName("query")
public class Query implements ShellTask {
    private final QueryManager query;
    private final ObjectMapper mapper;

    @Inject
    public Query(QueryManager query, @Named("application/json") ObjectMapper mapper) {
        this.query = query;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final String queryString = params.query.stream().collect(Collectors.joining(" "));
        final QueryContext queryContext = QueryContext.empty();

        final ObjectMapper indent = mapper.copy();
        indent.configure(SerializationFeature.INDENT_OUTPUT, true);

        final QueryOptions.Builder optionsBuilder =
            QueryOptions.builder().tracing(Tracing.fromBoolean(params.tracing));

        params.dataLimit.ifPresent(optionsBuilder::dataLimit);
        params.groupLimit.ifPresent(optionsBuilder::groupLimit);
        params.seriesLimit.ifPresent(optionsBuilder::seriesLimit);

        final Optional<QueryOptions> options = Optional.of(optionsBuilder.build());

        final Optional<QueryDateRange> range =
            Optional.of(new QueryDateRange.Relative(TimeUnit.DAYS, 1));

        QueryBuilder queryBuilder =
            query.newQueryFromString(queryString).options(options).rangeIfAbsent(range);
        if (params.slicedDataFetch) {
            queryBuilder.features(Optional.of(FeatureSet.of(Feature.SLICED_DATA_FETCH)));
        }
        return query
            .useGroup(params.group)
            .query(queryBuilder.build(), queryContext)
            .directTransform(result -> {
                for (final RequestError e : result.getErrors()) {
                    io.out().println(String.format("ERR: %s", e.toString()));
                }

                io.out().println(String.format("LIMITS: %s", result.getLimits().getLimits()));

                for (final ShardedResultGroup resultGroup : result.getGroups()) {
                    final MetricCollection group = resultGroup.getMetrics();

                    io
                        .out()
                        .println(String.format("%s: %s %s", group.getType(), resultGroup.getShard(),
                            indent.writeValueAsString(resultGroup.getSeries())));
                    io.out().println(indent.writeValueAsString(group.getData()));
                    io.out().flush();
                }

                io.out().println("TRACE:");
                result.getTrace().formatTrace(io.out());
                io.out().flush();

                return null;
            });
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private String group = null;

        @Argument(metaVar = "<query>")
        private List<String> query = new ArrayList<>();

        @Option(name = "--tracing", usage = "Enable extensive tracing")
        private boolean tracing = false;

        @Option(name = "--sliced-data-fetch", usage = "Enable sliced data fetch")
        private boolean slicedDataFetch = false;


        @Option(name = "--data-limit", usage = "Enable data limiting")
        private Optional<Long> dataLimit = Optional.empty();

        @Option(name = "--group-limit", usage = "Enable limiting number of groups")
        private Optional<Long> groupLimit = Optional.empty();

        @Option(name = "--series-limit", usage = "Enable number of series used")
        private Optional<Long> seriesLimit = Optional.empty();
    }

    public static Query setup(final CoreComponent core) {
        return DaggerQuery_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        Query task();
    }
}
