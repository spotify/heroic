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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.spotify.heroic.HeroicCore.Builder;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.elasticsearch.TransportClientSetup;
import com.spotify.heroic.elasticsearch.index.SingleIndexMapping;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metadata.elasticsearch.ElasticsearchMetadataModule;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.HeroicShellBridge;
import com.spotify.heroic.shell.QuoteParser;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.ShellTaskParams;
import com.spotify.heroic.shell.ShellTasks;
import com.spotify.heroic.shell.ShellTasks.TaskDefinition;
import com.spotify.heroic.shell.ShellTasks.TaskShellDefinition;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.suggest.SuggestManagerModule;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.suggest.elasticsearch.ElasticsearchSuggestModule;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Slf4j
public class HeroicShell implements HeroicShellBridge {
    public static final Path[] DEFAULT_CONFIGS = new Path[] { Paths.get("heroic.yml"),
            Paths.get("/etc/heroic/heroic.yml") };

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
        final ParsedArguments parsed = ParsedArguments.parse(args);

        try {
            parser.parseArgument(parsed.primary);
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

        if (parsed.child.isEmpty()) {
            interactive(params, builder);
            System.exit(0);
            return;
        }

        try {
            standalone(parsed.child, builder);
        } catch (Exception e) {
            log.error("Failed to run standalone task", e);
        }

        System.exit(0);
    }

    static void interactive(Parameters params, HeroicCore.Builder builder) {
        final HeroicCore core = builder.build();

        log.info("Starting Heroic...");

        try {
            core.start();
        } catch (final Exception e) {
            log.error("Failed to start core", e);
            System.exit(1);
            return;
        }

        log.info("Setting up interactive shell...");

        final SortedMap<String, TaskShellDefinition> tasks;

        try {
            tasks = new TreeMap<>(buildTasks(core));
        } catch (final Exception e) {
            log.error("Failed to build tasks", e);
            return;
        }

        final HeroicShell shell = core.inject(new HeroicShell(tasks));

        try {
            shell.run();
        } catch (final Exception e) {
            log.error("Exception caught while running shell", e);
        }

        try {
            core.shutdown();
        } catch (final Exception e) {
            log.error("Failed to stop runner in a timely fashion", e);
        }
    }

    static void standalone(List<String> arguments, Builder builder) throws Exception {
        final String taskName = arguments.iterator().next();
        final List<String> rest = arguments.subList(1, arguments.size());

        log.info("Running standalone task {}", taskName);

        final Class<ShellTask> taskType = resolveShellTask(taskName);
        final ShellTask task = ShellTasks.instance(taskType);
        final ShellTaskParams params = task.params();

        final CmdLineParser parser = new CmdLineParser(params);

        try {
            parser.parseArgument(rest);
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

        if (params instanceof Tasks.ElasticSearchParams) {
            final Tasks.ElasticSearchParams elasticsearch = (Tasks.ElasticSearchParams) params;

            if (elasticsearch.getSeeds() != null) {
                log.info("Setting up standalone elasticsearch configuration");
                standaloneElasticsearchConfig(builder, elasticsearch);
            }
        }

        final HeroicCore core = builder.build();

        log.info("Starting Heroic...");

        try {
            core.start();
        } catch (Exception e) {
            log.error("Failed to start core", e);
            System.exit(1);
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

        try {
            core.shutdown();
        } catch (final Exception e) {
            log.error("Failed to stop runner in a timely fashion", e);
        }
    }

    @SuppressWarnings("unchecked")
    static Class<ShellTask> resolveShellTask(final String taskName) throws ClassNotFoundException, Exception {
        final Class<?> taskType = Class.forName(taskName);

        if (!(ShellTask.class.isAssignableFrom(taskType))) {
            throw new Exception(String.format("Not an instance of ShellTask (%s)", taskName));
        }

        return (Class<ShellTask>) taskType;
    }

    final SortedMap<String, ShellTasks.TaskShellDefinition> tasks;

    boolean running = true;

    @Inject
    AsyncFramework async;

    // mutable state, a.k.a. settings
    int currentTimeout = 10;

    public HeroicShell(final SortedMap<String, ShellTasks.TaskShellDefinition> tasks) {
        this.tasks = tasks;
    }

    @Override
    public void exit() {
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

            final SortedMap<String, ShellTasks.Task> tasks = setupTasks(reader);

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

    SortedMap<String, ShellTasks.Task> setupTasks(final ConsoleReader reader) {
        final SortedMap<String, ShellTasks.Task> tasks = new TreeMap<>();

        for (Entry<String, ShellTasks.TaskShellDefinition> e : this.tasks.entrySet()) {
            tasks.put(e.getKey(), e.getValue().setup(this, reader, tasks));
        }

        return tasks;
    }

    void doReaderLoop(final ConsoleReader reader, SortedMap<String, ShellTasks.Task> tasks) throws Exception {
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

    @Override
    public void internalTimeoutTask(PrintWriter out, String[] args) {
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
            final SortedMap<String, ShellTasks.Task> tasks) throws Exception, IOException {
        // TODO: improve this with proper quote parsing and such.
        final String taskName = parts[0];
        final String[] args = extractArgs(parts);

        final ShellTasks.Task task = resolveTask(out, taskName, tasks);

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

    ShellTasks.Task resolveTask(final PrintWriter out, final String taskName,
            final SortedMap<String, ShellTasks.Task> tasks) {
        final SortedMap<String, ShellTasks.Task> selected = tasks.subMap(taskName, taskName + Character.MAX_VALUE);

        final ShellTasks.Task exact;

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

    @Override
    public void printTasksHelp(final PrintWriter out, final SortedMap<String, ShellTasks.Task> tasks) {
        for (final Map.Entry<String, ShellTasks.Task> e : tasks.entrySet()) {
            out.println(String.format("%s - %s", e.getKey(), e.getValue().usage()));
        }
    }

    AsyncFuture<Void> runTask(PrintWriter out, ShellTasks.Task task, String args[]) throws Exception {
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

    static void standaloneElasticsearchConfig(HeroicCore.Builder builder, Tasks.ElasticSearchParams params) {
        final List<String> seeds = Arrays.asList(StringUtils.split(params.getSeeds(), ','));

        final String clusterName = params.getClusterName();
        final String backendType = params.getBackendType();

        builder.profile(new HeroicProfile() {

            @Override
            public HeroicConfig build() throws Exception {
                // @formatter:off

                final TransportClientSetup clientSetup = TransportClientSetup.builder()
                    .clusterName(clusterName)
                    .seeds(seeds)
                .build();

                return HeroicConfig.builder()
                        .metadata(
                            MetadataManagerModule.builder()
                            .backends(
                                ImmutableList.<MetadataModule>of(
                                    ElasticsearchMetadataModule.builder()
                                    .connection(setupConnection(clientSetup, "metadata"))
                                    .writesPerSecond(0d)
                                    .build()
                                )
                            ).build()
                        )
                        .suggest(
                            SuggestManagerModule.builder()
                            .backends(
                                ImmutableList.<SuggestModule>of(
                                    ElasticsearchSuggestModule.builder()
                                    .connection(setupConnection(clientSetup, "suggest"))
                                    .writesPerSecond(0d)
                                    .backendType(backendType)
                                    .build()
                                )
                            )
                            .build()
                        )

                .build();
                // @formatter:on
            }

            private ManagedConnectionFactory setupConnection(TransportClientSetup clientSetup, final String index) {
                // @formatter:off
                return ManagedConnectionFactory.builder()
                    .clientSetup(clientSetup)
                    .index(SingleIndexMapping.builder().index(index).build())
                    .build();
                // @formatter:on
            }

            @Override
            public String description() {
                return "load metadata form a file";
            }
        });
    }

    static PrintWriter standaloneOutput(final ShellTaskParams params, final PrintStream original) throws IOException {
        if (params.output() != null && !"-".equals(params.output())) {
            return new PrintWriter(Files.newOutputStream(Paths.get(params.output())));
        }

        return new PrintWriter(original);
    }

    static Map<String, TaskShellDefinition> buildTasks(HeroicCore core) throws Exception {
        final Map<String, TaskShellDefinition> tasks = new HashMap<>();

        for (final Entry<String, TaskDefinition> e : ShellTasks.available().entrySet()) {
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

    @RequiredArgsConstructor
    static class ParsedArguments {
        final List<String> primary;
        final List<String> child;

        public static ParsedArguments parse(String[] args) {
            final List<String> primary = new ArrayList<>();
            final List<String> child = new ArrayList<>();

            final Iterator<String> iterator = Arrays.stream(args).iterator();

            while (iterator.hasNext()) {
                final String arg = iterator.next();

                if ("--".equals(arg)) {
                    break;
                }

                primary.add(arg);
            }

            while (iterator.hasNext()) {
                child.add(iterator.next());
            }

            return new ParsedArguments(primary, child);
        }
    }
}
