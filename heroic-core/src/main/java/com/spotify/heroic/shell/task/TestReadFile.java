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

import com.google.common.base.Charsets;
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
import org.kohsuke.args4j.Option;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

@TaskUsage("(Test) Test to read lines from a file")
@TaskName("test-read-file")
public class TestReadFile implements ShellTask {
    private final AsyncFramework async;

    @Inject
    public TestReadFile(AsyncFramework async) {
        this.async = async;
    }

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        try (final BufferedReader in = new BufferedReader(
            new InputStreamReader(io.newInputStream(params.in, StandardOpenOption.READ),
                Charsets.UTF_8))) {

            long i = 0;

            while (true) {
                final String line = in.readLine();

                if (line == null) {
                    break;
                }

                io.out().println(String.format("Read: %d", i++));
            }
        }

        return async.resolved();
    }

    static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-i", aliases = "--in", usage = "File to read from", metaVar = "<file>")
        private Path in = Paths.get("in.txt");
    }

    public static TestReadFile setup(final CoreComponent core) {
        return DaggerTestReadFile_C.builder().coreComponent(core).build().task();
    }

    @Component(dependencies = CoreComponent.class)
    interface C {
        TestReadFile task();
    }
}
