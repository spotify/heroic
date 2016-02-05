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

package com.spotify.heroic;

import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.ShellTaskDefinition;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.protocol.CommandDefinition;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.zip.GZIPOutputStream;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class HeroicShellTasks {
    public static final Joiner joiner = Joiner.on(", ");

    final List<ShellTaskDefinition> available;
    final SortedMap<String, ShellTask> tasks;
    final AsyncFramework async;

    public List<CommandDefinition> commands() {
        final List<CommandDefinition> commands = new ArrayList<>();

        for (final ShellTaskDefinition def : available) {
            commands.add(new CommandDefinition(def.name(), def.aliases(), def.usage()));
        }

        return commands;
    }

    public AsyncFuture<Void> evaluate(List<String> command, ShellIO io) throws Exception {
        if (command.isEmpty()) {
            return async.failed(new Exception("Empty command"));
        }

        final String taskName = command.iterator().next();
        final List<String> args = command.subList(1, command.size());

        final ShellTask task;

        try {
            task = resolveTask(taskName);
        } catch (final Exception e) {
            return async.failed(e);
        }

        final TaskParameters params = task.params();

        if (params != null) {
            final CmdLineParser parser = new CmdLineParser(params);

            try {
                parser.parseArgument(args);
            } catch (CmdLineException e) {
                return async.failed(e);
            }

            if (params.help()) {
                parser.printUsage(io.out(), null);
                return async.resolved();
            }
        }

        if (params.output() == null || "-".equals(params.output())) {
            if (params.gzip()) {
                io.out().println("--gzip: only works in combination with (-o/--output <file>)");
                io.out().flush();
            }

            return runTaskWithIO(task, io, params);
        }

        io.out().println("Writing output (-o/--output) to: " + params.output());
        io.out().flush();
        return runWithRedirectedIO(io, task, params);
    }

    private AsyncFuture<Void> runWithRedirectedIO(ShellIO io, final ShellTask task,
            final TaskParameters params) throws IOException {
        final PrintWriter out = new PrintWriter(
                new OutputStreamWriter(setupOutputStream(io, params), Charsets.UTF_8));

        final ShellIO wrapIO = new ShellIO() {
            @Override
            public PrintWriter out() {
                return out;
            }

            @Override
            public OutputStream newOutputStream(Path path, StandardOpenOption... options)
                    throws IOException {
                return io.newOutputStream(path, options);
            }

            @Override
            public InputStream newInputStream(Path path, StandardOpenOption... options)
                    throws IOException {
                return io.newInputStream(path, options);
            }
        };

        return runTaskWithIO(task, wrapIO, params).onFinished(out::close);
    }

    private OutputStream setupOutputStream(final ShellIO io, final TaskParameters params)
            throws IOException {
        final OutputStream out = io.newOutputStream(Paths.get(params.output()));

        if (!params.gzip()) {
            return out;
        }

        return new GZIPOutputStream(out);
    }

    private AsyncFuture<Void> runTaskWithIO(final ShellTask task, final ShellIO io,
            final TaskParameters params) {
        try {
            return task.run(io, params);
        } catch (Exception e) {
            return async.failed(e);
        }
    }

    ShellTask resolveTask(final String taskName) {
        final SortedMap<String, ShellTask> selected =
                tasks.subMap(taskName, taskName + Character.MAX_VALUE);

        final ShellTask exact;

        // exact match
        if ((exact = selected.get(taskName)) != null) {
            return exact;
        }

        // no fuzzy matches
        if (selected.isEmpty()) {
            throw new IllegalArgumentException(String.format("No task matching (%s)", taskName));
        }

        if (selected.size() > 1) {
            throw new IllegalArgumentException(String.format("Too many (%d) matching tasks (%s)",
                    selected.size(), joiner.join(selected.keySet())));
        }

        return selected.values().iterator().next();
    }
}
