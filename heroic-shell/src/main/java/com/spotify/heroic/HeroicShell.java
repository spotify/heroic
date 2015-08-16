/* Copyright (c) 2015 Spotify AB.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License. */

package com.spotify.heroic;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import jline.console.ConsoleReader;
import jline.console.UserInterruptException;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;
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
import com.spotify.heroic.shell.Tasks;
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
import com.spotify.heroic.shell.task.Query;
import com.spotify.heroic.shell.task.SuggestKey;
import com.spotify.heroic.shell.task.SuggestPerformance;
import com.spotify.heroic.shell.task.SuggestTag;
import com.spotify.heroic.shell.task.SuggestTagKeyCount;
import com.spotify.heroic.shell.task.SuggestTagValue;
import com.spotify.heroic.shell.task.SuggestTagValues;
import com.spotify.heroic.shell.task.Write;
import com.spotify.heroic.shell.task.WritePerformance;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Slf4j
public class HeroicShell {
    public static final Path[] DEFAULT_CONFIGS = new Path[] { Paths.get("heroic.yml"),
            Paths.get("/etc/heroic/heroic.yml") };

    private static final Map<String, TaskDefinition> available = new HashMap<>();

    static {
        // built-in
        available.put("exit", builtin("exit shell", (shell, reader, out, tasks, args) -> shell.exit()));
        available.put("help",
                builtin("print help", (shell, reader, out, tasks, args) -> shell.printTasksHelp(out, tasks)));
        available.put(
                "timeout",
                builtin("get/set shell timeout",
                        (shell, reader, out, tasks, args) -> shell.internalTimeoutTask(out, args)));
        available
                .put("clear", builtin("clear shell screen", (shell, reader, out, tasks, args) -> reader.clearScreen()));

        available.put("configure", shellTask(Configure.class));
        available.put("get", shellTask(ConfigGet.class));
        available.put("keys", shellTask(Keys.class));
        available.put("backends", shellTask(ListBackends.class));
        available.put("fetch", shellTask(Fetch.class));
        available.put("write", shellTask(Write.class));
        available.put("write-performance", shellTask(WritePerformance.class));
        available.put("metadata-delete", shellTask(MetadataDelete.class));
        available.put("metadata-fetch", shellTask(MetadataFetch.class));
        available.put("metadata-tags", shellTask(MetadataTags.class));
        available.put("metadata-count", shellTask(MetadataCount.class));
        available.put("metadata-entries", shellTask(MetadataEntries.class));
        available.put("metadata-migrate", shellTask(MetadataMigrate.class));
        available.put("metadata-migrate-suggestions", shellTask(MetadataMigrateSuggestions.class));
        available.put("metadata-load", shellTask(MetadataLoad.class));
        available.put("suggest-tag", shellTask(SuggestTag.class));
        available.put("suggest-key", shellTask(SuggestKey.class));
        available.put("suggest-tag-value", shellTask(SuggestTagValue.class));
        available.put("suggest-tag-values", shellTask(SuggestTagValues.class));
        available.put("suggest-tag-key-count", shellTask(SuggestTagKeyCount.class));
        available.put("suggest-performance", shellTask(SuggestPerformance.class));
        available.put("query", shellTask(Query.class));
    }

    public static void main(String[] args) throws IOException {
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                try {
                    log.error("Uncaught exception in thread {}, exiting...", t, e);
                } finally {
                    System.exit(1);
                }
            }
        });

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
            HeroicModules.printProfileUsage(new PrintWriter(System.err), "-P");
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

        final SortedMap<String, TaskShellDefinition> tasks;

        try {
            tasks = new TreeMap<>(buildTasks(core));
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
        Thread.setDefaultUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread t, Throwable e) {
                try {
                    log.error("Uncaught exception in thread {}, exiting...", t, e);
                } finally {
                    System.exit(1);
                }
            }
        });

        final ShellTask task = instance(taskType);

        final ShellTaskParams params = task.params();

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
            HeroicModules.printProfileUsage(System.err, "-P");
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

    final SortedMap<String, TaskShellDefinition> tasks;

    boolean running = true;

    @Inject
    AsyncFramework async;

    // mutable state, a.k.a. settings
    int currentTimeout = 10;

    public HeroicShell(final SortedMap<String, TaskShellDefinition> tasks) {
        this.tasks = tasks;
    }

    void exit() {
        this.running = false;
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

            final ArrayList<String> commands = new ArrayList<>(tasks.keySet());

            reader.addCompleter(new StringsCompleter(commands));
            reader.setHandleUserInterrupt(true);

            final SortedMap<String, Task> tasks = setupTasks(reader);

            try {
                doReaderLoop(reader, tasks);
            } catch (Exception e) {
                log.error("Error in reader loop", e);
            }

            if (history != null) {
                history.flush();
            }

            reader.shutdown();
        }
    }

    SortedMap<String, Task> setupTasks(final ConsoleReader reader) {
        final SortedMap<String, Task> tasks = new TreeMap<>();

        for (Entry<String, TaskShellDefinition> e : this.tasks.entrySet()) {
            tasks.put(e.getKey(), e.getValue().setup(this, reader, tasks));
        }

        return tasks;
    }

    void doReaderLoop(final ConsoleReader reader, SortedMap<String, Task> tasks) throws Exception {
        final PrintWriter out = new PrintWriter(reader.getOutput());

        while (running) {
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

            final String[] parts;

            try {
                parts = QuoteParser.parse(line);
            } catch (Exception e) {
                log.error("Line syntax invalid", e);
                continue;
            }

            if (parts.length == 0) {
                continue;
            }

            final long start = System.nanoTime();
            runTask(reader, parts, out, tasks);
            final long diff = System.nanoTime() - start;

            out.println(String.format("time: %s", Tasks.formatTimeNanos(diff)));
        }

        out.println();
        out.println("Exiting...");
    }

    void internalTimeoutTask(PrintWriter out, String[] args) {
        if (args.length < 2) {
            if (currentTimeout == 0) {
                out.println("timeout disabled");
            } else {
                out.println(String.format("timeout = %d", currentTimeout));
            }

            return;
        }

        final int timeout;

        try {
            timeout = Integer.parseInt(args[1]);
        } catch (Exception e) {
            out.println(String.format("not a valid integer value: %s", args[1]));
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

    void runTask(final ConsoleReader reader, String[] parts, final PrintWriter out,
            final SortedMap<String, Task> tasks) throws Exception, IOException {
        // TODO: improve this with proper quote parsing and such.
        final String taskName = parts[0];
        final String[] args = extractArgs(parts);

        final Task task = resolveTask(out, taskName, tasks);

        if (task == null) {
            return;
        }

        final AsyncFuture<Void> t;

        try {
            t = runTask(out, task, args);
        } catch (Exception e) {
            out.println("Command failed");
            e.printStackTrace(out);
            return;
        }

        if (t == null) {
            out.flush();
            return;
        }

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

    Task resolveTask(final PrintWriter out, final String taskName,
            final SortedMap<String, Task> tasks) {
        final SortedMap<String, Task> selected = tasks.subMap(taskName, taskName + Character.MAX_VALUE);

        final Task exact;

        // exact match
        if ((exact = selected.get(taskName)) != null) {
            return exact;
        }

        // no fuzzy matches
        if (selected.isEmpty()) {
            out.println("No such task '" + taskName + "'");
            return null;
        }

        if (selected.size() > 1) {
            out.println(String.format("Too many (%d) matching tasks:", selected.size()));
            printTasksHelp(out, selected);

            return null;
        }
        return selected.values().iterator().next();
    }

    Void awaitFinished(final AsyncFuture<Void> t) throws InterruptedException, ExecutionException, TimeoutException {
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

    void printTasksHelp(final PrintWriter out, final SortedMap<String, Task> tasks) {
        for (final Map.Entry<String, Task> e : tasks.entrySet()) {
            out.println(String.format("%s - %s", e.getKey(), e.getValue().usage()));
        }
    }

    AsyncFuture<Void> runTask(PrintWriter out, Task task, String args[]) throws Exception {
        final ShellTaskParams params = task.params();

        if (params != null) {
            final CmdLineParser parser = new CmdLineParser(params);

            try {
                parser.parseArgument(args);
            } catch (CmdLineException e) {
                log.error("Commandline error", e);
                return async.failed(e);
            }

            if (params.help()) {
                parser.printUsage(out, null);
                return async.resolved();
            }
        }

        return task.run(out, args, params);
    }

    static PrintWriter standaloneOutput(final ShellTaskParams params, final PrintStream original) throws IOException {
        if (params.output() != null && !"-".equals(params.output())) {
            return new PrintWriter(Files.newOutputStream(Paths.get(params.output())));
        }

        return new PrintWriter(original);
    }

    static Map<String, TaskShellDefinition> buildTasks(HeroicCore core) throws Exception {
        final Map<String, TaskShellDefinition> tasks = new HashMap<>();

        for (final Entry<String, TaskDefinition> e : HeroicShell.available.entrySet()) {
            tasks.put(e.getKey(), e.getValue().setup(core));
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

    static TaskDefinition builtin(final String usage, final Builtin command) {
        return new TaskDefinition() {
            @Override
            public TaskShellDefinition setup(HeroicCore core) throws Exception {
                return new TaskShellDefinition() {
                    @Override
                    public Task setup(final HeroicShell shell, final ConsoleReader reader,
                            final SortedMap<String, Task> tasks) {
                        return core.inject(new Task() {
                            @Inject
                            AsyncFramework async;

                            @Override
                            public ShellTaskParams params() {
                                return null;
                            }

                            @Override
                            public String usage() {
                                return usage;
                            }

                            @Override
                            public AsyncFuture<Void> run(PrintWriter out, String[] args, ShellTaskParams params)
                                    throws Exception {
                                try {
                                    command.run(shell, reader, out, tasks, args);
                                } catch (Exception e) {
                                    return async.failed(e);
                                }

                                return async.resolved();
                            }
                        });
                    }
                };
            }
        };
    }

    static TaskDefinition shellTask(final Class<? extends ShellTask> task) {
        return new TaskDefinition() {
            @Override
            public TaskShellDefinition setup(final HeroicCore core) throws Exception {
                final ShellTask instance = core.inject(instance(task));

                final String usage;

                final ShellTaskUsage u = task.getAnnotation(ShellTaskUsage.class);

                if (u == null) {
                    usage = String.format("<no @ShellTaskUsage annotation for %s>", task.getName());
                } else {
                    usage = u.value();
                }

                return new TaskShellDefinition() {
                    @Override
                    public Task setup(HeroicShell shell, ConsoleReader reader,
                            SortedMap<String, Task> tasks) {
                        return new Task() {
                            @Override
                            public ShellTaskParams params() {
                                return instance.params();
                            }

                            @Override
                            public String usage() {
                                return usage;
                            }

                            @Override
                            public AsyncFuture<Void> run(PrintWriter out, String args[], ShellTaskParams params)
                                    throws Exception {
                                return instance.run(out, params);
                            }
                        };
                    }
                };
            };
        };
    }

    @ToString
    public static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--server", usage = "Start shell as server (enables listen port)")
        private boolean server = false;
    }

    public static interface Builtin {
        void run(HeroicShell shell, ConsoleReader reader, PrintWriter out,
                SortedMap<String, Task> tasks, String[] args) throws Exception;
    }

    public static interface TaskDefinition {
        TaskShellDefinition setup(HeroicCore core) throws Exception;
    }

    public static interface TaskShellDefinition {
        Task setup(HeroicShell shell, ConsoleReader reader,
                SortedMap<String, Task> tasks);
    }

    public static interface Task {
        ShellTaskParams params();

        String usage();

        AsyncFuture<Void> run(PrintWriter out, String[] args, ShellTaskParams params) throws Exception;
    }
}
