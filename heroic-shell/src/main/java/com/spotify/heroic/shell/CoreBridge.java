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

package com.spotify.heroic.shell;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.StringUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.HeroicCore.Builder;
import com.spotify.heroic.HeroicModules;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Borrowed;
import eu.toolchain.async.Managed;

@Slf4j
@RequiredArgsConstructor
public class CoreBridge {
    public static final Path[] DEFAULT_CONFIGS = new Path[] { Paths.get("heroic.yml"),
            Paths.get("/etc/heroic/heroic.yml") };

    private final Managed<State> state;

    public String status() throws Exception {
        try (final Borrowed<State> b = state.borrow()) {
            return b.get().status.call();
        }
    }

    public void start() throws Exception {
        state.start().get();
    }

    public void stop() throws Exception {
        state.stop().get();
    }

    public Task setup(Class<? extends Task> taskType) throws Exception {
        final Task task = instance(taskType);

        try (final Borrowed<State> b = state.borrow()) {
            b.get().core.inject(task);
        }

        return task;
    }

    public AsyncFuture<Void> run(Task task, String argv[], PrintWriter out) throws Exception {
        final BaseParams params = task.params();

        try {
            parseArguments(params, argv, out);
        } catch (CmdLineException e) {
            log.error("Commandline error", e);
            return null;
        }

        if (params.help())
            return null;

        return task.run(out, params);
    }

    public static void standalone(String argv[], Class<? extends Task> taskType) throws Exception {
        final Task task = instance(taskType);

        final BaseParams params = task.params();

        try (final PrintWriter out = new PrintWriter(System.out)) {
            try {
                parseArguments(params, argv, out);
            } catch (CmdLineException e) {
                log.error("Commandline error", e);
                return;
            }

            if (params.help())
                return;

            final HeroicCore core = HeroicCore.builder().server(false).configPath(parseConfigPath(params.config()))
                    .build();

            try {
                core.start();
            } catch (Exception e1) {
                log.error("Failed to start core");
                core.shutdown();
                return;
            }

            core.inject(task);

            try {
                task.run(out, params).get();
            } catch (Exception e) {
                log.error("Failed to run task", e);
            }

            core.shutdown();
        }
    }

    private static Path parseConfigPath(String config) {
        final Path path = doParseConfigPath(config);

        if (!Files.isRegularFile(path))
            throw new IllegalStateException("No such file: " + path.toAbsolutePath());

        return path;
    }

    private static Path doParseConfigPath(String config) {
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

    private static String formatDefaults(Path[] defaultConfigs) {
        final List<Path> alternatives = new ArrayList<>(defaultConfigs.length);

        for (final Path path : defaultConfigs)
            alternatives.add(path.toAbsolutePath());

        return StringUtils.join(alternatives, ", ");
    }

    private static void parseArguments(BaseParams params, String[] args, PrintWriter out) throws CmdLineException,
            IOException {
        final CmdLineParser parser = new CmdLineParser(params);

        parser.parseArgument(args);

        if (params.help())
            parser.printUsage(out, null);
    }

    public static interface Task {
        public BaseParams params();

        public AsyncFuture<Void> run(PrintWriter out, BaseParams params) throws Exception;
    }

    public static interface BaseParams {
        public String config();

        public boolean help();
    }

    private static Task instance(Class<? extends Task> taskType) throws Exception {
        final Constructor<? extends Task> constructor;

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

    @Data
    public static final class State {
        private final HeroicCore core;
        private final Callable<String> status;
    }

    public static Builder setupBuilder(boolean server, String config) {
        return HeroicCore.builder().server(server).configPath(CoreBridge.parseConfigPath(config))
                .modules(HeroicModules.ALL_MODULES)
                .oneshot(true);
    }
}
