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

package com.spotify.heroic.shell.task.datamigrate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.Tracing;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.shell.task.parameters.DataMigrateParameters;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import javax.inject.Inject;
import javax.inject.Named;

@TaskUsage("Migrate data from one backend to another")
@TaskName("data-migrate")
public class DataMigrate implements ShellTask {
    public static final long DOTS = 100;
    public static final long LINES = DOTS * 20;
    public static final long ALLOWED_ERRORS = 5;
    public static final long ALLOWED_FAILED_KEYS = 100;

    private final QueryParser parser;
    private final MetricManager metric;
    private final AsyncFramework async;
    private final ObjectMapper mapper;

    @Inject
    public DataMigrate(
        QueryParser parser, MetricManager metric, AsyncFramework async,
        @Named("application/json") ObjectMapper mapper
    ) {
        this.parser = parser;
        this.metric = metric;
        this.async = async;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new DataMigrateParameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters p) throws Exception {
        final DataMigrateParameters params = (DataMigrateParameters) p;

        final QueryOptions.Builder options =
            QueryOptions.builder().tracing(Tracing.fromBoolean(params.getTracing()));

        params.getFetchSize().ifPresent(options::fetchSize);

        final Filter filter = Tasks.setupFilter(parser, params);
        final MetricBackend from = metric.useOptionalGroup(params.getFrom());
        final MetricBackend to = metric.useOptionalGroup(params.getTo());

        final BackendKeyFilter keyFilter = Tasks.setupKeyFilter(params, mapper);

        final ResolvableFuture<Void> future = async.future();

        /* all errors seen */
        final ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        final AsyncObservable<BackendKeySet> observable;

        if (params.getKeysPaged()) {
            observable = from.streamKeysPaged(keyFilter, options.build(), params.getKeysPageSize());
        } else {
            observable = from.streamKeys(keyFilter, options.build());
        }

        observable.observe(
            new KeyObserver(io, params, filter, from, to, future, errors, async, mapper));

        return future.directTransform(v -> {
            io.out().println();

            if (!errors.isEmpty()) {
                io.out().println("ERRORS: ");

                for (final Throwable t : errors) {
                    io.out().println(t.getMessage());
                    t.printStackTrace(io.out());
                }
            }

            io.out().flush();
            return null;
        });
    }

    public static DataMigrate setup(final CoreComponent core) {
        return DaggerDataMigrate_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        DataMigrate task();
    }
}
