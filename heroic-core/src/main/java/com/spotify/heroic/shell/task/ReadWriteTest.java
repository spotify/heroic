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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.kohsuke.args4j.Option;

import com.google.common.base.Charsets;
import com.google.inject.Inject;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskName;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.TaskUsage;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.ToString;

@TaskUsage("Test to read and write a file")
@TaskName("read-write-test")
public class ReadWriteTest implements ShellTask {
    private static final Charset UTF8 = Charsets.UTF_8;

    @Inject
    private AsyncFramework async;

    @Override
    public TaskParameters params() {
        return new Parameters();
    }

    @Override
    public AsyncFuture<Void> run(final ShellIO io, final TaskParameters base) throws Exception {
        final Parameters params = (Parameters) base;

        final Path p = Paths.get(params.file);

        try (final BufferedWriter writer =
                new BufferedWriter(new OutputStreamWriter(io.newOutputStream(p), UTF8))) {
            writer.write("Hello World\n");
        }

        try (final BufferedReader reader =
                new BufferedReader(new InputStreamReader(io.newInputStream(p), UTF8))) {
            io.out().println(reader.readLine());
        }

        io.out().flush();
        return async.resolved();
    }

    @ToString
    private static class Parameters extends AbstractShellTaskParams {
        @Option(name = "-f", aliases = {"--file"}, usage = "File to perform test against",
                metaVar = "<file>")
        private String file = null;
    }
}
