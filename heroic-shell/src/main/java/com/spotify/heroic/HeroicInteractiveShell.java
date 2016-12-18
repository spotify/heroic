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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.spotify.heroic.shell.CoreInterface;
import com.spotify.heroic.shell.HeroicCompleter;
import com.spotify.heroic.shell.HeroicParser;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.SyntaxError;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.shell.protocol.Command;
import com.spotify.heroic.shell.protocol.CommandsResponse;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.jline.reader.Completer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.InfoCmp;

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@RequiredArgsConstructor
public class HeroicInteractiveShell {
    private static final String PROMPT = "heroic> ";
    private static final String HEROIC_HISTORY = ".heroic_history";

    private static final String EXIT = "exit";
    private static final String HELP = "help";
    private static final String CLEAR = "clear";
    private static final String TIMEOUT = "timeout";

    private static final Joiner COMMAND_JOINER = Joiner.on(" ");

    public static final List<Command> BUILTIN = ImmutableList.of(
        new Command(HELP, ImmutableList.of(), "Show help", ImmutableList.of(), ImmutableList.of()),
        new Command(CLEAR, ImmutableList.of(), "Clear the current shell", ImmutableList.of(),
            ImmutableList.of()),
        new Command(TIMEOUT, ImmutableList.of(), "Get or set the current task timeout",
            ImmutableList.of(),
            ImmutableList.of(new Command.Arg("timeout", new Command.Type.Number(), false, true))),
        new Command(EXIT, ImmutableList.of(), "Exit the shell", ImmutableList.of(),
            ImmutableList.of()));

    final Optional<History> history;
    final Terminal terminal;
    final LineReader reader;
    final List<Command> commands;
    final FileInputStream input;

    // settings
    int timeout = 10;

    public void run(final CoreInterface core) throws Exception {
        final PrintWriter out = terminal.writer();

        outer:
        while (true) {
            final String raw;

            try {
                raw = reader.readLine(PROMPT);
            } catch (final UserInterruptException e) {
                out.println("INT");
                break;
            } catch (final EndOfFileException e) {
                out.println("EOF");
                break;
            } catch (final SyntaxError syntax) {
                out.println(String.format("%d:%d: %s", syntax.getLine(), syntax.getCol(),
                    syntax.getMessage()));

                final String[] lines = syntax.getInput().split("\n");
                out.println(lines[syntax.getLine()]);
                out.println(StringUtils.repeat(' ', syntax.getCol()) + "^");

                continue;
            }

            if (raw == null) {
                break;
            }

            final ParsedLine parsed = reader.getParsedLine();

            final Iterable<List<String>> commands = HeroicParser.splitCommands(parsed.words());

            for (final List<String> words : commands) {
                if (words.isEmpty()) {
                    continue;
                }

                final String commandName = words.iterator().next();

                if (EXIT.equals(commandName)) {
                    break outer;
                }

                if (HELP.equals(commandName)) {
                    printHelp(out);
                    continue;
                }

                if (CLEAR.equals(commandName)) {
                    terminal.puts(InfoCmp.Capability.clear_screen);
                    continue;
                }

                if (TIMEOUT.equals(commandName)) {
                    internalTimeoutTask(out, words);
                    continue;
                }

                final ShellIO io = new DirectShellIO(out);

                final long start = System.nanoTime();

                try {
                    runTask(words, io, core);
                } catch (final Exception e) {
                    e.printStackTrace(out);
                }

                final long diff = System.nanoTime() - start;
                out.println(String.format("time: %s", Tasks.formatTimeNanos(diff)));
            }
        }

        out.println();
        out.println("Exiting...");
    }

    public void shutdown() {
        history.ifPresent(History::save);
    }

    void printHelp(PrintWriter out) {
        out.println("Built-in:");

        for (final Command c : BUILTIN) {
            printCommandUsage(out, c);
        }

        out.println();

        out.println("Commands:");

        for (final Command c : commands) {
            printCommandUsage(out, c);
        }
    }

    void printCommandUsage(final PrintWriter out, final Command c) {
        final List<String> parts = new ArrayList<>();
        parts.add(c.getName());

        c.getArguments().forEach(argument -> {
            parts.add(argument.display());
        });

        final String commandSpec = COMMAND_JOINER.join(parts);

        out.println(String.format("%s - %s", commandSpec, c.getUsage()));

        if (!c.getAliases().isEmpty()) {
            out.println(String.format("  aliases: %s", StringUtils.join(", ", c.getAliases())));
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

    void runTask(List<String> command, final ShellIO io, final CoreInterface core)
        throws Exception {
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

    static HeroicInteractiveShell buildInstance(
        final CommandsResponse commandsResponse, FileInputStream input
    ) throws Exception {
        final List<Command> commands = commandsResponse.getCommands();
        final SortedSet<String> groups = commandsResponse.getGroups();
        final Completer completer =
            new HeroicCompleter(ImmutableList.copyOf(Iterables.concat(BUILTIN, commands)), groups);

        final FileOutputStream output = new FileOutputStream(FileDescriptor.out);
        final Terminal terminal = buildTerminal(input, output);

        final Parser parser = new HeroicParser();

        final LineReaderBuilder readerBuilder =
            LineReaderBuilder.builder().terminal(terminal).completer(completer).parser(parser);

        final Optional<Path> historyPath = historyPath();

        final Optional<History> history = historyPath.map(path -> new DefaultHistory());

        historyPath.ifPresent(
            path -> readerBuilder.variable(LineReader.HISTORY_FILE, path.toAbsolutePath()));

        history.ifPresent(readerBuilder::history);

        return new HeroicInteractiveShell(history, terminal, readerBuilder.build(), commands,
            input);
    }

    private static Terminal buildTerminal(
        final FileInputStream input, final FileOutputStream output
    ) throws IOException {
        final TerminalBuilder terminalBuilder =
            TerminalBuilder.builder().system(true).streams(input, output);
        term().ifPresent(terminalBuilder::type);
        return terminalBuilder.build();
    }

    private static Optional<String> term() throws IOException {
        return Optional.ofNullable(System.getenv("TERM"));
    }

    private static Optional<Path> historyPath() throws IOException {
        final String home = System.getProperty("user.home");

        if (home == null) {
            return Optional.empty();
        }

        return Optional.of(Paths.get(home, HEROIC_HISTORY));
    }
}
