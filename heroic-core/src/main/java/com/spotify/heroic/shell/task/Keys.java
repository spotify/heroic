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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.async.AsyncObservable;
import com.spotify.heroic.async.AsyncObserver;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.metric.BackendKey;
import com.spotify.heroic.metric.BackendKeyFilter;
import com.spotify.heroic.metric.BackendKeySet;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.Tracing;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.shell.task.parameters.KeysParameters;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import java.util.concurrent.atomic.AtomicLong;
import javax.inject.Inject;
import javax.inject.Named;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TaskUsage("List available metric keys for all backends")
@TaskName("keys")
public class Keys implements ShellTask {
    private static final Logger log = LoggerFactory.getLogger(Keys.class);
    private final AsyncFramework async;
    private final MetricManager metrics;
    private final ObjectMapper mapper;

    @Inject
    public Keys(
        AsyncFramework async, MetricManager metrics, @Named("application/json") ObjectMapper mapper
    ) {
        this.async = async;
        this.metrics = metrics;
        this.mapper = mapper;
    }

    @Override
    public TaskParameters params() {
        return new KeysParameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final KeysParameters params = (KeysParameters) base;

        final BackendKeyFilter keyFilter = Tasks.setupKeyFilter(params, mapper);

        final QueryOptions.Builder options =
            QueryOptions.builder().tracing(Tracing.fromBoolean(params.getTracing()));

        params.getFetchSize().ifPresent(options::fetchSize);

        final MetricBackendGroup group = metrics.useOptionalGroup(params.getGroup());

        final ResolvableFuture<Void> future = async.future();

        final AsyncObservable<BackendKeySet> observable;

        if (params.getKeysPaged()) {
            observable = group.streamKeysPaged(
                keyFilter, options.build(), params.getKeysPageSize());
        } else {
            observable = group.streamKeys(keyFilter, options.build());
        }

        observable.observe(new AsyncObserver<BackendKeySet>() {
            final AtomicLong failedKeys = new AtomicLong();
            final AtomicLong total = new AtomicLong();

            @Override
            public AsyncFuture<Void> observe(BackendKeySet keys) {
                failedKeys.addAndGet(keys.getFailedKeys());
                total.addAndGet(keys.getKeys().size() + keys.getFailedKeys());

                for (final BackendKey key : keys.getKeys()) {
                    try {
                        io.out().println(mapper.writeValueAsString(key));
                    } catch (final JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                }

                io.out().flush();
                return async.resolved();
            }

            @Override
            public void cancel() {
                log.error("Cancelled");
                end();
            }

            @Override
            public void fail(final Throwable cause) {
                log.warn("Exception when pulling keys", cause);
                end();
            }

            @Override
            public void end() {
                io.out().println("Failed Keys: " + failedKeys.get() + "/" + total.get());
                future.resolve(null);
            }
        });

        return future;
    }

    public static Keys setup(final CoreComponent core) {
        return DaggerKeys_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        Keys task();
    }
}
