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

import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.Map.Entry;
import java.util.Set;
import javax.inject.Inject;

@TaskUsage("Get local statistics")
@TaskName("statistics")
public class Statistics implements ShellTask {
    private final AsyncFramework async;
    private final Set<Consumer> consumers;
    private final IngestionManager ingestion;
    private final MetricManager metrics;
    private final MetadataManager metadata;
    private final SuggestManager suggest;

    @Inject
    public Statistics(
        AsyncFramework async, Set<Consumer> consumers, IngestionManager ingestion,
        MetricManager metrics, MetadataManager metadata, SuggestManager suggest
    ) {
        this.async = async;
        this.consumers = consumers;
        this.ingestion = ingestion;
        this.metrics = metrics;
        this.metadata = metadata;
        this.suggest = suggest;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        io.out().println("Consumers:");

        for (final Consumer c : consumers) {
            io.out().println("  " + c.toString());

            for (final Entry<String, Long> e : c.getStatistics().getCounters().entrySet()) {
                io.out().println("    " + e.getKey() + "=" + e.getValue());
            }
        }

        io.out().println("Ingestion: " + ingestion);

        for (final Entry<String, Long> e : ingestion.getStatistics().getCounters().entrySet()) {
            io.out().println("  " + e.getKey() + "=" + e.getValue());
        }

        io.out().println("Metrics:");

        for (final MetricBackend m : metrics.groupSet().useAll().getMembers()) {
            io.out().println("  " + m.toString());

            for (final Entry<String, Long> e : m.getStatistics().getCounters().entrySet()) {
                io.out().println("    " + e.getKey() + "=" + e.getValue());
            }
        }

        io.out().println("Metadata:");

        for (final MetadataBackend m : metadata.groupSet().useAll().getMembers()) {
            io.out().println("  " + m.toString());

            for (final Entry<String, Long> e : m.getStatistics().getCounters().entrySet()) {
                io.out().println("    " + e.getKey() + "=" + e.getValue());
            }
        }

        io.out().println("Suggest:");

        for (final SuggestBackend s : suggest.groupSet().useAll().getMembers()) {
            io.out().println("  " + s.toString());

            for (final Entry<String, Long> e : s.getStatistics().getCounters().entrySet()) {
                io.out().println("    " + e.getKey() + "=" + e.getValue());
            }
        }

        io.out().flush();
        return async.resolved();
    }

    private static class Parameters extends AbstractShellTaskParams {
    }

    public static Statistics setup(final CoreComponent core) {
        return DaggerStatistics_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        Statistics task();
    }
}
