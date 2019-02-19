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

import com.google.common.collect.ImmutableList;
import com.google.protobuf.ProtocolStringList;
import com.spotify.heroic.proto.ShellMessage.CommandsResponse.CommandDefinition;
import com.spotify.heroic.shell.CoreInterface;
import com.spotify.heroic.shell.QuoteParser;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.Tasks;
import eu.toolchain.async.AsyncFuture;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class HeroicInteractiveShell {
    final ConsoleReader reader;
    final List<CommandDefinition> commands;
    final FileHistory history;

    boolean running = true;

    // mutable state, a.k.a. settings
    int timeout = 10;

    @java.beans.ConstructorProperties({ "reader", "commands", "history" })
    public HeroicInteractiveShell(final ConsoleReader reader,
                                  final List<CommandDefinition> commands,
                                  final FileHistory history) {
        this.reader = reader;
        this.commands = commands;
        this.history = history;
    }

    public void run(final CoreInterface core) throws Exception {
        final PrintWriter out = new PrintWriter(reader.getOutput());

        while (running) {
            final String raw;

            try {
                raw = reader.readLine();
            } catch (UserInterruptException e) {
                out.println("Interrupted");
                break;
            }

            if (raw == null) {
                break;
            }

            final List<List<String>> lines;

            try {
                lines = QuoteParser.parse(raw);
            } catch (Exception e) {
                log.error("Line syntax invalid", e);
                return;
            }

            lines.forEach(command -> {
                if (command.isEmpty()) {
                    return;
                }

                final String commandName = command.iterator().next();

                if ("exit".equals(commandName)) {
                    running = false;
                    return;
                }

                if ("help".equals(commandName)) {
                    printTasksHelp(out);
                    return;
                }

                if ("clear".equals(commandName)) {
                    try {
                        reader.clearScreen();
                    } catch (IOException e) {
                        log.error("Failed to clear screen", e);
                    }

                    return;
                }

                if ("timeout".equals(commandName)) {
                    internalTimeoutTask(out, command);
                    return;
                }

                final ShellIO io = new DirectShellIO(out);

                final long start = System.nanoTime();

                try {
                    runTask(command, io, core);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                final long diff = System.nanoTime() - start;

                out.println(String.format("time: %s", Tasks.formatTimeNanos(diff)));
            });
        }

        out.println();
        out.println("Exiting...");
    }

    public void shutdown() throws IOException {
        if (history != null) {
            history.flush();
        }

        reader.shutdown();
    }

    void printTasksHelp(PrintWriter out) {
        out.println("Available commands:");

        for (final CommandDefinition cmd : commands) {
            out.println(String.format("%s - %s", cmd.getName(), cmd.getUsage()));

            final ProtocolStringList aliases = cmd.getAliasesList();
            if (!aliases.isEmpty()) {
                out.println(String.format("  aliases: %s", StringUtils.join(", ", aliases)));
            }
        }
    }

    void internalTimeoutTask(PrintWriter out, List<String> args) {
        if (args.size() < 2) {
            if (this.timeout == 0) {
                out.println("timeout disabled");
            } else {
                out.println(String.format("timeout = %d", this.timeout));
            }

            return;
        }

        final int timeout;

        try {
            timeout = Integer.parseInt(args.get(1));
        } catch (Exception e) {
            out.println(String.format("not a valid integer value: %s", args.get(1)));
            return;
        }

        if (timeout <= 0) {
            out.println("Timeout disabled");
            this.timeout = 0;
        } else {
            out.println(String.format("Timeout updated to %d seconds", timeout));
            this.timeout = timeout;
        }
    }

    void runTask(List<String> command, final ShellIO io, final CoreInterface core) {
        final AsyncFuture<Void> t;

        try {
            t = core.evaluate(command, io);
        } catch (Exception e) {
            io.out().println("Command failed");
            e.printStackTrace(io.out());
            return;
        }

        if (t == null) {
            io.out().flush();
            return;
        }

        try {
            awaitFinished(t);
        } catch (TimeoutException e) {
            io.out().println(String.format("Command timed out (current timeout = %ds)", timeout));
            t.cancel(true);
        } catch (Exception e) {
            io.out().println("Command failed");
            e.printStackTrace(io.out());
            return;
        }

        io.out().flush();
    }

    Void awaitFinished(final AsyncFuture<Void> t)
        throws InterruptedException, ExecutionException, TimeoutException {
        if (timeout > 0) {
            return t.get(timeout, TimeUnit.SECONDS);
        }

        log.warn(String.format("Waiting forever for task (timeout = %d)", timeout));
        return t.get();
    }

    public static HeroicInteractiveShell buildInstance(
        final List<CommandDefinition> commands, FileInputStream input
    ) throws Exception {
        final ConsoleReader reader = new ConsoleReader("heroicsh", input, System.out, null);

        final FileHistory history = setupHistory(reader);

        if (history != null) {
            reader.setHistory(history);
        }

        reader.setPrompt(String.format("heroic> "));
        reader.addCompleter(new StringsCompleter(
            ImmutableList.copyOf(commands.stream().map((d) -> d.getName()).iterator())));
        reader.setHandleUserInterrupt(true);

        return new HeroicInteractiveShell(reader, commands, history);
    }

    private static FileHistory setupHistory(final ConsoleReader reader) throws IOException {
        final String home = System.getProperty("user.home");

        if (home == null) {
            return null;
        }

        return new FileHistory(new File(home, ".heroicsh-history"));
    }
}
