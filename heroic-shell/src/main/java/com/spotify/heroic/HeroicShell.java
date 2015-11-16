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

import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.spotify.heroic.HeroicCore.Builder;
import com.spotify.heroic.shell.AbstractShellTaskParams;
import com.spotify.heroic.shell.CoreInterface;
import com.spotify.heroic.shell.RemoteCoreInterface;
import com.spotify.heroic.shell.ShellIO;
import com.spotify.heroic.shell.ShellProtocol;
import com.spotify.heroic.shell.ShellTask;
import com.spotify.heroic.shell.TaskParameters;
import com.spotify.heroic.shell.Tasks;
import com.spotify.heroic.shell.protocol.CommandDefinition;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.TinyAsync;
import eu.toolchain.serializer.SerializerFramework;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HeroicShell {
    public static final Path[] DEFAULT_CONFIGS =
            new Path[] {Paths.get("heroic.yml"), Paths.get("/etc/heroic/heroic.yml")};

    public static final SerializerFramework serializer = ShellProtocol.setupSerializer();

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
            parser.printUsage(System.out);
            System.out.println();
            HeroicModules.printAllUsage(System.out, "-P");
            System.exit(0);
            return;
        }

        final AsyncFramework async =
                TinyAsync.builder().executor(Executors.newSingleThreadExecutor()).build();

        if (parsed.child.isEmpty()) {
            final CoreInterface bridge;

            try {
                bridge = setupCoreBridge(params, async);
            } catch (Exception e) {
                log.error("Failed to setup core bridge", e);
                System.exit(1);
                return;
            }

            interactive(params, bridge);
            System.exit(0);
            return;
        }

        final HeroicCore.Builder builder = setupBuilder(params);

        try {
            standalone(parsed.child, builder);
        } catch (Exception e) {
            log.error("Failed to run standalone task", e);
        }

        System.exit(0);
    }

    private static CoreInterface setupCoreBridge(Parameters params, AsyncFramework async)
            throws Exception {
        if (params.connect != null) {
            return setupRemoteCore(params.connect, async);
        }

        return setupLocalCore(params, async);
    }

    private static CoreInterface setupRemoteCore(String connect, AsyncFramework async)
            throws Exception {
        return RemoteCoreInterface.fromConnectString(connect, async, serializer);
    }

    private static CoreInterface setupLocalCore(Parameters params, AsyncFramework async)
            throws Exception {
        final HeroicCore.Builder builder = setupBuilder(params);

        final HeroicCore core = builder.build();

        log.info("Starting local Heroic...");

        final HeroicCoreInstance instance = core.start();

        return instance.inject(new CoreInterface() {
            @Inject
            public HeroicShellTasks tasks;

            @Override
            public AsyncFuture<Void> evaluate(List<String> command, ShellIO io) throws Exception {
                return tasks.evaluate(command, io);
            }

            @Override
            public List<CommandDefinition> commands() throws Exception {
                return tasks.commands();
            }

            @Override
            public void shutdown() throws Exception {
                instance.shutdown();
            }
        });
    }

    static void interactive(Parameters params, CoreInterface core) {
        log.info("Setting up interactive shell...");

        try {
            runInteractiveShell(core);
        } catch (Exception e) {
            log.error("Error when running shell", e);
        }

        log.info("Closing core bridge...");

        try {
            core.shutdown();
        } catch (Exception e) {
            log.error("Failed to close core bridge", e);
        }
    }

    static void runInteractiveShell(final CoreInterface core) throws Exception {
        final List<CommandDefinition> commands = new ArrayList<>(core.commands());

        commands.add(new CommandDefinition("clear", ImmutableList.of(), "Clear the current shell"));
        commands.add(new CommandDefinition("timeout", ImmutableList.of(),
                "Get or set the current task timeout"));
        commands.add(new CommandDefinition("exit", ImmutableList.of(), "Exit the shell"));

        try (final FileInputStream input = new FileInputStream(FileDescriptor.in)) {
            final HeroicInteractiveShell interactive =
                    HeroicInteractiveShell.buildInstance(commands, input);

            try {
                interactive.run(core);
            } finally {
                interactive.shutdown();
            }
        }
    }

    static void standalone(List<String> arguments, Builder builder) throws Exception {
        final String taskName = arguments.iterator().next();
        final List<String> rest = arguments.subList(1, arguments.size());

        log.info("Running standalone task {}", taskName);

        final Class<ShellTask> taskType = resolveShellTask(taskName);
        final ShellTask task = Tasks.newInstance(taskType);
        final TaskParameters params = task.params();

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
            HeroicModules.printAllUsage(System.err, "-P");
            System.exit(0);
            return;
        }

        final HeroicCore core = builder.build();

        log.info("Starting Heroic...");
        final HeroicCoreInstance instance = core.start();

        try {
            instance.inject(task);

            final PrintWriter o = standaloneOutput(params, System.out);
            final ShellIO io = new DirectShellIO(o);

            try {
                task.run(io, params).get();
            } catch (Exception e) {
                log.error("Failed to run task", e);
            } finally {
                o.flush();
            }
        } finally {
            instance.shutdown();
        }
    }

    @SuppressWarnings("unchecked")
    static Class<ShellTask> resolveShellTask(final String taskName)
            throws ClassNotFoundException, Exception {
        final Class<?> taskType = Class.forName(taskName);

        if (!(ShellTask.class.isAssignableFrom(taskType))) {
            throw new Exception(String.format("Not an instance of ShellTask (%s)", taskName));
        }

        return (Class<ShellTask>) taskType;
    }

    static PrintWriter standaloneOutput(final TaskParameters params, final PrintStream original)
            throws IOException {
        final OutputStream out;

        if (params.output() != null && !"-".equals(params.output())) {
            out = Files.newOutputStream(Paths.get(params.output()));
        } else {
            out = original;
        }

        return new PrintWriter(new OutputStreamWriter(out, Charsets.UTF_8));
    }

    static Path parseConfigPath(String config) {
        final Path path = doParseConfigPath(config);

        if (!Files.isRegularFile(path)) {
            throw new IllegalStateException("No such file: " + path.toAbsolutePath());
        }

        return path;
    }

    static Path doParseConfigPath(String config) {
        if (config == null) {
            for (final Path p : DEFAULT_CONFIGS) {
                if (Files.isRegularFile(p)) {
                    return p;
                }
            }

            throw new IllegalStateException("No default configuration available, checked "
                    + formatDefaults(DEFAULT_CONFIGS));
        }

        return Paths.get(config);
    }

    static String formatDefaults(Path[] defaultConfigs) {
        final List<Path> alternatives = new ArrayList<>(defaultConfigs.length);

        for (final Path path : defaultConfigs) {
            alternatives.add(path.toAbsolutePath());
        }

        return StringUtils.join(alternatives, ", ");
    }

    static HeroicCore.Builder setupBuilder(Parameters params) {
        HeroicCore.Builder builder = HeroicCore.builder().setupService(params.server)
                .disableBackends(params.disableBackends).skipLifecycles(params.skipLifecycles)
                .modules(HeroicModules.ALL_MODULES).oneshot(true);

        if (params.config() != null) {
            builder.configPath(parseConfigPath(params.config()));
        }

        builder.parameters(ExtraParameters.ofList(params.parameters));

        for (final String profile : params.profiles()) {
            final HeroicProfile p = HeroicModules.PROFILES.get(profile);

            if (p == null) {
                throw new IllegalArgumentException(
                        String.format("not a valid profile: %s", profile));
            }

            builder.profile(p);
        }

        builder.setupShellServer(params.shellServer);

        return builder;
    }

    @ToString
    public static class Parameters extends AbstractShellTaskParams {
        @Option(name = "--server", usage = "Start shell as server (enables listen port)")
        private boolean server = false;

        @Option(name = "--shell-server",
                usage = "Start shell with shell server (enables remote connections)")
        private boolean shellServer = false;

        @Option(name = "--skip-lifecycles", usage = "Start core without starting lifecycles")
        private boolean skipLifecycles = false;

        @Option(name = "--disable-backends", usage = "Start core without configuring backends")
        private boolean disableBackends = false;

        @Option(name = "--connect", usage = "Connect to a remote heroic server",
                metaVar = "<host>[:<port>]")
        private String connect = null;

        @Option(name = "-X", usage = "Define an extra parameter", metaVar = "<key>=<value>")
        private final List<String> parameters = new ArrayList<>();
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
