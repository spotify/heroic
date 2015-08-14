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

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.inject.Inject;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.QuoteParser;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.ShellTaskParams;
import com.spotify.heroic.shell.ShellTaskUsage;
import com.spotify.heroic.shell.task.ConfigGet;
import com.spotify.heroic.shell.task.Configure;
import com.spotify.heroic.shell.task.Fetch;
import com.spotify.heroic.shell.task.Keys;
import com.spotify.heroic.shell.task.ListBackends;
import com.spotify.heroic.shell.task.MetadataCount;
import com.spotify.heroic.shell.task.MetadataDelete;
import com.spotify.heroic.shell.task.MetadataEntries;
import com.spotify.heroic.shell.task.MetadataFetch;
import com.spotify.heroic.shell.task.MetadataLoad;
import com.spotify.heroic.shell.task.MetadataMigrate;
import com.spotify.heroic.shell.task.MetadataMigrateSuggestions;
import com.spotify.heroic.shell.task.MetadataTags;
import com.spotify.heroic.shell.task.SuggestKey;
import com.spotify.heroic.shell.task.SuggestPerformance;
import com.spotify.heroic.shell.task.SuggestTag;
import com.spotify.heroic.shell.task.SuggestTagKeyCount;
import com.spotify.heroic.shell.task.SuggestTagValue;
import com.spotify.heroic.shell.task.SuggestTagValues;
import com.spotify.heroic.shell.task.WriteEvents;
import com.spotify.heroic.shell.task.WritePerformance;
import com.spotify.heroic.shell.task.WritePoints;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Slf4j
public class HeroicShell {
    public static final Path[] DEFAULT_CONFIGS = new Path[] { Paths.get("heroic.yml"),
            Paths.get("/etc/heroic/heroic.yml") };

    private static final Map<String, Class<? extends ShellTask>> available = new HashMap<>();

    static {
        available.put("configure", Configure.class);
        available.put("get", ConfigGet.class);
        available.put("keys", Keys.class);
        available.put("backends", ListBackends.class);
        available.put("fetch", Fetch.class);
        available.put("write-points", WritePoints.class);
        available.put("write-events", WriteEvents.class);
        available.put("write-performance", WritePerformance.class);
        available.put("metadata-delete", MetadataDelete.class);
        available.put("metadata-fetch", MetadataFetch.class);
        available.put("metadata-tags", MetadataTags.class);
        available.put("metadata-count", MetadataCount.class);
        available.put("metadata-entries", MetadataEntries.class);
        available.put("metadata-migrate", MetadataMigrate.class);
        available.put("metadata-migrate-suggestions", MetadataMigrateSuggestions.class);
        available.put("metadata-load", MetadataLoad.class);
        available.put("suggest-tag", SuggestTag.class);
        available.put("suggest-key", SuggestKey.class);
        available.put("suggest-tag-value", SuggestTagValue.class);
        available.put("suggest-tag-values", SuggestTagValues.class);
        available.put("suggest-tag-key-count", SuggestTagKeyCount.class);
        available.put("suggest-performance", SuggestPerformance.class);
    }

    public static void main(String[] args) throws IOException {
        final Parameters params = new Parameters();

        final CmdLineParser parser = new CmdLineParser(params);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            log.error("Argument error", e);
            System.exit(1);
            return;
        }

        if (params.help()) {
            parser.printUsage(System.err);
            HeroicModules.printProfileUsage(new PrintWriter(System.err), "-p");
            System.exit(0);
            return;
        }

        final HeroicCore.Builder builder = setupBuilder(params.server, params.config(), params.profile());
        final HeroicCore core = builder.build();

        log.info("Starting Heroic...");

        try {
            core.start();
        } catch (Exception e) {
            log.error("Failed to start core", e);
            System.exit(1);
            return;
        }

        log.info("Wiring tasks...");

        final Map<String, ShellTask> tasks;

        try {
            tasks = buildTasks(core);
        } catch (Exception e) {
            log.error("Failed to build tasks", e);
            return;
        }

        final HeroicShell shell = core.inject(new HeroicShell(tasks));

        try {
            shell.run();
        } catch (Exception e) {
            log.error("Exception caught while running shell", e);
        }

        try {
            core.shutdown();
        } catch (Exception e) {
            log.error("Failed to stop runner in a timely fashion", e);
        }

        System.exit(0);
    }

    /**
     * Entrypoint for running standalone tasks.
     *
     * @param argv
     * @param taskType
     * @throws Exception
     */
    public static void standalone(String argv[], Class<? extends ShellTask> taskType) throws Exception {
        final ShellTask task = instance(taskType);

        final ShellTaskParams params = task.params();

        final PrintWriter out = new PrintWriter(System.out);

        final CmdLineParser parser = new CmdLineParser(params);

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            log.error("Error parsing arguments", e);
            System.exit(1);
            return;
        }

        if (params.help()) {
            parser.printUsage(System.err);
            HeroicModules.printProfileUsage(System.err, "-p");
            System.exit(0);
            return;
        }

        final HeroicCore.Builder builder = setupBuilder(false, params.config(), params.profile());

        task.standaloneConfig(builder, params);

        final HeroicCore core = builder.build();

        try {
            core.start();
        } catch (Exception e) {
            log.error("Failed to start core", e);
            return;
        }

        try {
            core.inject(task);
        } catch (final Exception e) {
            log.error("Failed to inject task", e);
            return;
        }

        final PrintWriter o = standaloneOutput(params, System.out);

        try {
            task.run(o, params).get();
        } catch (Exception e) {
            log.error("Failed to run task", e);
        } finally {
            o.flush();
        }

        log.info("Exiting...");
        core.shutdown();
        System.exit(0);
    }

    final Map<String, ShellTask> tasks;

    @Inject
    AsyncFramework async;

    // mutable state, a.k.a. settings
    int currentTimeout = 10;

    public HeroicShell(final Map<String, ShellTask> tasks) {
        this.tasks = tasks;
    }

    void run() throws IOException {
        try (final FileInputStream input = new FileInputStream(FileDescriptor.in)) {
            final ConsoleReader reader = new ConsoleReader("heroicsh", input, System.out, null);

            final String home = System.getProperty("user.home");

            final FileHistory history;

            if (home != null) {
                history = new FileHistory(new File(home, ".heroicsh-history"));
                reader.setHistory(history);
            } else {
                history = null;
            }

            final ArrayList<String> commands = new ArrayList<>(available.keySet());

            commands.add("exit");
            commands.add("clear");
            commands.add("help");
            commands.add("timeout");

            reader.addCompleter(new StringsCompleter(commands));
            reader.setHandleUserInterrupt(true);

            try {
                doReaderLoop(reader);
            } catch (Exception e) {
                log.error("Error in reader loop", e);
            }

            if (history != null)
                history.flush();

            reader.shutdown();
        }
    }

    void doReaderLoop(final ConsoleReader reader)
            throws Exception {
        final PrintWriter out = new PrintWriter(reader.getOutput());

        while (true) {
            reader.setPrompt(String.format("heroic> "));

            final String line;

            try {
                line = reader.readLine();
            } catch (UserInterruptException e) {
                out.println("Interrupted");
                break;
            }

            if (line == null)
                break;

            final String[] parts = QuoteParser.parse(line);

            if (parts.length == 0)
                continue;

            if ("exit".equals(parts[0]))
                break;

            if ("clear".equals(parts[0])) {
                reader.clearScreen();
                continue;
            }

            if ("help".equals(parts[0])) {
                printHelp(tasks, out);
                continue;
            }

            if ("timeout".equals(parts[0])) {
                internalTimeoutTask(parts, out);
                continue;
            }

            final long start = System.nanoTime();
            runTask(reader, parts, out);
            final long diff = System.nanoTime() - start;

            out.println(String.format("time: %s", formatTime(diff)));
        }

        out.println();
        out.println("Exiting...");
    }

    void internalTimeoutTask(String[] parts, PrintWriter out) {
        if (parts.length < 2) {
            if (currentTimeout == 0) {
                out.println("timeout disabled");
            } else {
                out.println(String.format("timeout = %d", currentTimeout));
            }

            return;
        }

        final int timeout;

        try {
            timeout = Integer.parseInt(parts[1]);
        } catch (Exception e) {
            out.println(String.format("not a valid integer value: %s", parts[1]));
            return;
        }

        if (timeout <= 0) {
            out.println("Timeout disabled");
            currentTimeout = 0;
        } else {
            out.println(String.format("Timeout updated to %d seconds", timeout));
            currentTimeout = timeout;
        }
    }

    String formatTime(long diff) {
        if (diff < 1000) {
            return String.format("%d ns", diff);
        }

        if (diff < 1000000) {
            final double v = ((double) diff) / 1000;
            return String.format("%.3f us", v);
        }

        if (diff < 1000000000) {
            final double v = ((double) diff) / 1000000;
            return String.format("%.3f ms", v);
        }

        final double v = ((double) diff) / 1000000000;
        return String.format("%.3f s", v);
    }

    void printHelp(final Map<String, ShellTask> tasks, final PrintWriter out) {
        out.println("Known Commands:");
        out.println("help - Display this help");
        out.println("exit - Exit the shell");
        out.println("clear - Clear the shell");
        out.println("timeout - Manipulate and get timeout settings");

        for (final Map.Entry<String, ShellTask> e : tasks.entrySet()) {
            final ShellTaskUsage usage = e.getValue().getClass().getAnnotation(ShellTaskUsage.class);

            final String help;

            if (usage == null) {
                help = "";
            } else {
                help = usage.value();
            }

            out.println(String.format("%s - %s", e.getKey(), help));
        }
    }

    void runTask(final ConsoleReader reader, String[] parts, final PrintWriter out) throws Exception,
            IOException {
        // TODO: improve this with proper quote parsing and such.
        final String taskName = parts[0];
        final String[] commandArgs = extractArgs(parts);

        final ShellTask task = tasks.get(taskName);

        if (task == null) {
            out.println("No such task '" + taskName + "'");
            return;
        }

        final AsyncFuture<Void> t;

        try {
            t = runTask(task, commandArgs, out);
        } catch (Exception e) {
            out.println("Command failed");
            e.printStackTrace(out);
            return;
        }

        if (t == null)
            return;

        try {
            awaitFinished(t);
        } catch (TimeoutException e) {
            out.println(String.format("Command timed out (current timeout = %ds)", currentTimeout));
            t.cancel(true);
        } catch (Exception e) {
            out.println("Command failed");
            e.printStackTrace(out);
            return;
        }

        out.flush();
        return;
    }

    Void awaitFinished(final AsyncFuture<Void> t) throws InterruptedException, ExecutionException,
            TimeoutException {
        if (currentTimeout > 0) {
            return t.get(currentTimeout, TimeUnit.SECONDS);
        }

        log.warn(String.format("Waiting forever for task (timeout = %d)", currentTimeout));
        return t.get();
    }

    String[] extractArgs(String[] parts) {
        if (parts.length <= 1)
            return new String[0];

        final String[] args = new String[parts.length - 1];

        for (int i = 1; i < parts.length; i++)
            args[i - 1] = parts[i];

        return args;
    }

    AsyncFuture<Void> runTask(ShellTask task, String argv[], PrintWriter out) throws Exception {
        final ShellTaskParams params = task.params();
        final CmdLineParser parser = new CmdLineParser(params);

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            log.error("Commandline error", e);
            return async.failed(e);
        }

        if (params.help()) {
            parser.printUsage(out, null);
            return async.resolved();
        }

        return task.run(out, params);
    }

    static PrintWriter standaloneOutput(final ShellTaskParams params, final PrintStream original) throws IOException {
        if (params.output() != null && !"-".equals(params.output())) {
            return new PrintWriter(Files.newOutputStream(Paths.get(params.output())));
        }

        return new PrintWriter(original);
    }

    static Map<String, ShellTask> buildTasks(HeroicCore core) throws Exception {
        final Map<String, ShellTask> tasks = new HashMap<>();

        for (final Entry<String, Class<? extends ShellTask>> e : HeroicShell.available.entrySet()) {
            tasks.put(e.getKey(), core.inject(instance(e.getValue())));
        }

        return tasks;
    }

    static Path parseConfigPath(String config) {
        final Path path = doParseConfigPath(config);

        if (!Files.isRegularFile(path))
            throw new IllegalStateException("No such file: " + path.toAbsolutePath());

        return path;
    }

    static Path doParseConfigPath(String config) {
        if (config == null) {
            for (final Path p : DEFAULT_CONFIGS) {
                if (Files.isRegularFile(p))
                    return p;
            }

            throw new IllegalStateException("No default configuration available, checked "
                    + formatDefaults(DEFAULT_CONFIGS));
        }

        return Paths.get(config);
    }

    static String formatDefaults(Path[] defaultConfigs) {
        final List<Path> alternatives = new ArrayList<>(defaultConfigs.length);

        for (final Path path : defaultConfigs)
            alternatives.add(path.toAbsolutePath());

        return StringUtils.join(alternatives, ", ");
    }

    static ShellTask instance(Class<? extends ShellTask> taskType) throws Exception {
        final Constructor<? extends ShellTask> constructor;

        try {
            constructor = taskType.getConstructor();
        } catch (ReflectiveOperationException e) {
            throw new Exception("Task '" + taskType.getCanonicalName()
                    + "' does not have an accessible, empty constructor", e);
        }

        try {
            return constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new Exception("Failed to invoke constructor of '" + taskType.getCanonicalName(), e);
        }
    }

    static HeroicCore.Builder setupBuilder(boolean server, String config, String profile) {
        HeroicCore.Builder builder = HeroicCore.builder().server(server).modules(HeroicModules.ALL_MODULES)
                .oneshot(true);

        if (config != null) {
            builder.configPath(parseConfigPath(config));
        }

        if (profile != null) {
            final HeroicProfile p = HeroicModules.PROFILES.get(profile);

            if (p == null)
                throw new IllegalArgumentException(String.format("not a valid profile: %s", profile));

            builder.profile(p);
        }

        return builder;
    }

    @ToString
    public static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--server", usage = "Start shell as server (enables listen port)")
        private boolean server = false;
    }

    @Data
    public static final class State {
        private final HeroicCore core;
        private final Callable<String> status;
    }
}
