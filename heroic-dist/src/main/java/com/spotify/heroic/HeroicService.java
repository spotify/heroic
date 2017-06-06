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

import com.spotify.heroic.HeroicCore.Builder;
import com.spotify.heroic.args4j.CmdLine;
import com.spotify.heroic.reflection.ResourceException;
import com.spotify.heroic.reflection.ResourceFileLoader;
import com.spotify.heroic.reflection.ResourceInstance;
import eu.toolchain.async.Transform;
import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.lang.Thread.UncaughtExceptionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class HeroicService {
    public static interface Configuration {
        void configure(HeroicCore.Builder builder, Parameters parameters);
    }

    public static final String CONFIGURATION_RESOURCE =
        "com.spotify.heroic.HeroicService.Configuration";

    /**
     * Default configuration path.
     */
    public static final Path DEFAULT_CONFIG = Paths.get("heroic.yml");

    public static void main(final String[] args) throws Exception {
        main(args, null);
    }

    public static void main(final String[] args, final Configuration configuration)
        throws Exception {
        HeroicLogging.configure();

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

        final HeroicCore core = builder.build();

        final HeroicCoreInstance instance;

        try {
            instance = core.newInstance();
        } catch (final Exception e) {
            log.error("Failed to create Heroic instance", e);
            System.exit(1);
            return;
        }

        try {
            instance.start().get();
        } catch (final Exception e) {
            log.error("Failed to start Heroic instance", e);
            System.exit(1);
            return;
        }

        final Thread shutdown = new Thread(instance::shutdown);
        shutdown.setName("heroic-shutdown-hook");

        /* setup shutdown hook */
        Runtime.getRuntime().addShutdownHook(shutdown);

        /* block until core stops */
        instance.join().get();

        log.info("Shutting down, bye bye!");
        System.exit(0);
    }

    private static void loadResourceConfigurations(final Builder builder, final Parameters params)
        throws ResourceException {
        final ClassLoader loader = Configuration.class.getClassLoader();

        final List<ResourceInstance<Configuration>> configs =
            ResourceFileLoader.loadInstances(CONFIGURATION_RESOURCE, loader, Configuration.class);

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

    private static void configureBuilder(
        final HeroicCore.Builder builder, final Parameters params
    ) {
        final Path config = getConfigPath(params.extra);

        // setup as a service accepting http requests.
        builder.setupService(true);

        // do not require a shell server by default.
        builder.setupShellServer(false);

        if (config != null) {
            builder.configPath(config);
        }

        if (params.id != null) {
            builder.id(params.id);
        }

        if (params.port != null) {
            builder.port(params.port);
        }

        if (params.host != null) {
            builder.host(params.host);
        }

        if (params.startupPing != null) {
            builder.startupPing(params.startupPing);
        }

        if (params.startupId != null) {
            builder.startupId(params.startupId);
        }

        for (final String profile : params.profiles) {
            builder.profile(setupProfile(profile));
        }

        builder.parameters(ExtraParameters.ofList(params.parameters));
        builder.modules(HeroicModules.ALL_MODULES);
    }

    private static HeroicProfile setupProfile(final String profile) {
        final HeroicProfile p = HeroicModules.PROFILES.get(profile);

        if (p == null) {
            throw new IllegalArgumentException("No such profile: " + profile);
        }

        log.info("Enabling Profile: {}", profile);
        return p;
    }

    private static Path getConfigPath(final List<String> extra) {
        if (extra.size() > 0) {
            return Paths.get(extra.get(0));
        }

        if (Files.isRegularFile(DEFAULT_CONFIG)) {
            return DEFAULT_CONFIG;
        }

        return null;
    }

    private static Parameters parseArguments(final String[] args) {
        final Parameters params = new Parameters();

        final CmdLineParser parser = CmdLine.createParser(params);

        try {
            parser.parseArgument(args);
        } catch (final CmdLineException e) {
            log.error("Error parsing arguments", e);
            return null;
        }

        if (params.help) {
            parser.printUsage(System.out);
            System.out.println();
            HeroicModules.printAllUsage(System.out, "-P");
            return null;
        }

        return params;
    }

    // @formatter:off
    @ToString
    @Data
    public static class Parameters {
        @Option(name = "-P", aliases = {"--profile"},
                usage = "Activate a pre-defined profile instead of a configuration file. Profiles" +
                        " are pre-defined configurations, useful for messing around with the " +
                        "system.")
        private List<String> profiles = new ArrayList<>();

        @Option(name = "--port", usage = "Port number to bind to")
        private Integer port = null;

        @Option(name = "--host", usage = "Host to bind to")
        private String host = null;

        @Option(name = "--id", usage = "Heroic identifier")
        private String id = null;

        @Option(name = "-h", aliases = {"--help"}, help = true, usage = "Display help.")
        private boolean help;

        @Option(name = "--startup-ping",
                usage = "Send a JSON frame to the given URI containing information about this " +
                        "host after it has started.")
        private String startupPing;

        @Option(name = "--startup-id", usage = "Explicit id of a specific startup instance.")
        private String startupId;

        @Option(name = "-X", usage = "Define an extra parameter", metaVar = "<key>=<value>")
        private List<String> parameters = new ArrayList<>();

        @Argument
        private List<String> extra = new ArrayList<>();
    }
    // @formatter:on
}
