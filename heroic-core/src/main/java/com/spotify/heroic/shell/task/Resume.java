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

import java.util.Set;

import org.kohsuke.args4j.Option;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Getter;
import lombok.ToString;

@TaskUsage("Resume (or Unpause) operation of local components")
@TaskName("resume")
public class Resume implements ShellTask {
    @Inject
    private AsyncFramework async;

    @Inject
    private Set<Consumer> consumers;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final ImmutableList.Builder<AsyncFuture<Void>> futures = ImmutableList.builder();

        if (!params.skipConsumers) {
            io.out().println("Resuming Consumers");

            for (final Consumer c : consumers) {
                futures.add(c.resume());
            }
        }

        return async.collectAndDiscard(futures.build());
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--skip-consumers", usage = "Do not resume consumers")
        @Getter
        private boolean skipConsumers = false;
    }
}
