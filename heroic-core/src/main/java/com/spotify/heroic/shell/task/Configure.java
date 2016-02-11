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

import com.spotify.heroic.analytics.MetricAnalytics;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.suggest.SuggestManager;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.List;

@TaskUsage("Configure the given group of metric backends")
@TaskName("configure")
public class Configure implements ShellTask {
    private final AsyncFramework async;
    private final MetricManager metrics;
    private final MetadataManager metadata;
    private final SuggestManager suggest;
    private final MetricAnalytics analytics;

    @Inject
    public Configure(
        AsyncFramework async, MetricManager metrics, MetadataManager metadata,
        SuggestManager suggest, MetricAnalytics analytics
    ) {
        this.async = async;
        this.metrics = metrics;
        this.metadata = metadata;
        this.suggest = suggest;
        this.analytics = analytics;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final List<AsyncFuture<Void>> futures = new ArrayList<>();

        futures.add(analytics.configure());

        if (!params.noData) {
            futures.add(metrics.useGroup(params.group).configure());
            futures.add(metadata.useGroup(params.group).configure());
            futures.add(suggest.useGroup(params.group).configure());
        }

        return async.collectAndDiscard(futures);
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private String group = null;

        @Option(name = "--no-data", usage = "Do not configure data backends")
        private boolean noData = false;
    }

    public static Configure setup(final CoreComponent core) {
        return DaggerConfigure_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        Configure task();
    }
}
