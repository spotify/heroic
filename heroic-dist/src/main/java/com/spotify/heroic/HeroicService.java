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

import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.spotify.heroic.HeroicCore.Builder;
import com.spotify.heroic.reflection.ResourceException;
import com.spotify.heroic.reflection.ResourceFileLoader;
import com.spotify.heroic.reflection.ResourceInstance;

import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class HeroicService {
    public static interface Configuration {
        void configure(HeroicCore.Builder builder, Parameters parameters);
    }

    public static final String CONFIGURATION_RESOURCE = "com.spotify.heroic.HeroicService.Configuration";

    /**
     * Default configuration path.
     */
    public static final Path DEFAULT_CONFIG = Paths.get("heroic.yml");

    public static void main(final String[] args) throws Exception {
        main(args, null);
    }

    public static void main(final String[] args, final Configuration configuration) throws Exception {
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

        final Parameters params = parseArguments(args);

        if (params == null) {
            System.exit(0);
            return;
        }

        final HeroicCore.Builder builder = HeroicCore.builder();

        configureBuilder(builder, params);

        try {
            loadResourceConfigurations(builder, params);
        } catch (final ResourceException e) {
            log.error("Failed to load configurators from resource: {}", e.getMessage(), e);
            throw e;
        }

        final HeroicCoreInstance instance;

        try {
            instance = builder.build().start();
        } catch (final Exception e) {
            log.error("Failed to start heroic", e);
            System.exit(1);
            return;
        }

        final Thread shutdown = new Thread() {
            @Override
            public void run() {
                instance.shutdown();
            }
        };

        shutdown.setName("heroic-shutdown-hook");

        /* setup shutdown hook */
        Runtime.getRuntime().addShutdownHook(shutdown);

        /* block until core stops */
        instance.join();
        System.exit(0);
    }

    private static void loadResourceConfigurations(final Builder builder, final Parameters params)
            throws ResourceException {
        final ClassLoader loader = Configuration.class.getClassLoader();

        final List<ResourceInstance<Configuration>> configs = ResourceFileLoader.loadInstances(
                CONFIGURATION_RESOURCE, loader,
                Configuration.class);

        for (final ResourceInstance<Configuration> config : configs) {
            config.invoke(new Transform<Configuration, Void>() {
                @Override
                public Void transform(Configuration result) throws Exception {
                    result.configure(builder, params);
                    return null;
                }
            });
        }
    }

    private static void configureBuilder(final HeroicCore.Builder builder, final Parameters params) {
        final Path config = getConfigPath(params.extra);

        builder.setupServer(true);

        if (config != null)
            builder.configPath(config);

        if (params.port != null)
            builder.port(params.port);

        if (params.host != null)
            builder.host(params.host);

        if (params.startupPing != null)
            builder.startupPing(params.startupPing);

        if (params.startupId != null)
            builder.startupId(params.startupId);

        for (final String profile : params.profiles) {
            builder.profile(setupProfile(profile));
        }

        builder.parameters(HeroicParameters.ofList(params.parameters));
        builder.modules(HeroicModules.ALL_MODULES);
    }

    private static HeroicProfile setupProfile(final String profile) {
        final HeroicProfile p = HeroicModules.PROFILES.get(profile);

        if (p == null)
            throw new IllegalArgumentException("No such profile: " + profile);

        log.info("Enabling Profile: {}", profile);
        return p;
    }

    private static Path getConfigPath(final List<String> extra) {
        if (extra.size() > 0)
            return Paths.get(extra.get(0));

        if (Files.isRegularFile(DEFAULT_CONFIG))
            return DEFAULT_CONFIG;

        return null;
    }

    private static Parameters parseArguments(final String[] args) {
        final Parameters params = new Parameters();

        final CmdLineParser parser = new CmdLineParser(params);

        try {
            parser.parseArgument(args);
        } catch (final CmdLineException e) {
            log.error("Error parsing arguments", e);
            return null;
        }

        if (params.help) {
            parser.printUsage(System.out);
            HeroicModules.printProfileUsage(System.out, "-p");
            return null;
        }

        return params;
    }

    @ToString
    @Data
    public static class Parameters {
        @Option(name = "-P", aliases = { "--profile" }, usage = "Activate a pre-defined profile instead of a configuration file. Profiles are pre-defined configurations, useful for messing around with the system.")
        private List<String> profiles = new ArrayList<>();

        @Option(name = "--port", usage = "Port number to bind to")
        private final Integer port = null;

        @Option(name = "--host", usage = "Host to bind to")
        private final String host = null;

        @Option(name = "--id", usage = "Heroic identifier")
        private final String id = null;

        @Option(name = "-h", aliases = { "--help" }, help = true, usage = "Display help.")
        private boolean help;

        @Option(name = "--startup-ping", usage = "Send a JSON frame to the given URI containing information about this host after it has started.")
        private String startupPing;

        @Option(name = "--startup-id", usage = "Explicit id of a specific startup instance.")
        private String startupId;

        @Option(name = "-X", usage="Define an extra parameter", metaVar="<key>=<value>")
        private final List<String> parameters = new ArrayList<>();

        @Argument
        private final List<String> extra = new ArrayList<>();
    }
}
