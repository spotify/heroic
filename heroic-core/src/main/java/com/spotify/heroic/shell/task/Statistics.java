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

import java.util.Map.Entry;
import java.util.Set;

import com.google.inject.Inject;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;

@TaskUsage("Get local statistics")
@TaskName("statistics")
public class Statistics implements ShellTask {
    @Inject
    private AsyncFramework async;

    @Inject
    private Set<Consumer> consumers;

    @Inject
    private IngestionManager ingestion;

    @Inject
    private MetricManager metrics;

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

        for (final MetricBackend m : metrics.allMembers()) {
            io.out().println("  " + m.toString());

            for (final Entry<String, Long> e : m.getStatistics().getCounters().entrySet()) {
                io.out().println("    " + e.getKey() + "=" + e.getValue());
            }
        }

        io.out().flush();
        return async.resolved();
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
    }
}
