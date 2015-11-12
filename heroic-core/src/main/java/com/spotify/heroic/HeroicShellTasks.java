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

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.google.common.base.Joiner;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.ShellTaskDefinition;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.protocol.CommandDefinition;

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
