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
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.consumer.Consumer;
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
import java.io.PrintWriter;
import java.util.List;
import java.util.Set;

@TaskUsage("List available backend groups")
@TaskName("backends")
public class ListBackends implements ShellTask {
    private final MetricManager metrics;
    private final MetadataManager metadata;
    private final SuggestManager suggest;
    private final Set<Consumer> consumers;
    private final MetricAnalytics metricAnalytics;
    private final AsyncFramework async;

    @Inject
    public ListBackends(
        MetricManager metrics, MetadataManager metadata, SuggestManager suggest,
        Set<Consumer> consumers, MetricAnalytics metricAnalytics, AsyncFramework async
    ) {
        this.metrics = metrics;
        this.metadata = metadata;
        this.suggest = suggest;
        this.consumers = consumers;
        this.metricAnalytics = metricAnalytics;
        this.async = async;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        printBackends(io.out(), "metric", metrics.use(params.group));
        printBackends(io.out(), "metadata", metadata.use(params.group));
        printBackends(io.out(), "suggest", suggest.use(params.group));
        printConsumers(io.out(), "consumers", consumers);

        io.out().println(String.format("metric-analytics: %s", metricAnalytics));

        return async.resolved(null);
    }

    private void printConsumers(PrintWriter out, String title, Set<Consumer> consumers) {
        if (consumers.isEmpty()) {
            out.println(String.format("%s: (empty)", title));
            return;
        }

        out.println(String.format("%s:", title));

        for (final Consumer c : consumers) {
            out.println(String.format("  %s", c));
        }
    }

    private void printBackends(PrintWriter out, String title, List<? extends Grouped> group) {
        if (group.isEmpty()) {
            out.println(String.format("%s: (empty)", title));
            return;
        }

        out.println(String.format("%s:", title));

        for (final Grouped grouped : group) {
            out.println(String.format("  %s %s", grouped.getGroups(), grouped));
        }
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-g", aliases = {"--group"}, usage = "Backend group to use",
            metaVar = "<group>")
        private String group;
    }

    public static ListBackends setup(final CoreComponent core) {
        return DaggerListBackends_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        ListBackends task();
    }
}
