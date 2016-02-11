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

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;
import dagger.Component;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.Getter;
import lombok.ToString;
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import java.util.Set;

@TaskUsage("Pause operation of local components")
@TaskName("pause")
public class Pause implements ShellTask {
    private final AsyncFramework async;
    private final Set<Consumer> consumers;

    @Inject
    public Pause(AsyncFramework async, Set<Consumer> consumers) {
        this.async = async;
        this.consumers = consumers;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final ImmutableList.Builder<AsyncFuture<Void>> futures = ImmutableList.builder();

        if (!params.skipConsumers) {
            io.out().println("Pausing Consumers");

            for (final Consumer c : consumers) {
                futures.add(c.pause());
            }
        }

        return async.collectAndDiscard(futures.build());
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--skip-consumers", usage = "Do not pause consumers")
        @Getter
        private boolean skipConsumers = false;
    }

    public static Pause setup(final CoreComponent core) {
        return DaggerPause_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    static interface C {
        Pause task();
    }
}
