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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Data;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.profile.GeneratedProfile;

@Slf4j
public class HeroicService {
    public static interface Configuration {
        void configure(HeroicCore.Builder builder, Parameters parameters);
    }

    /**
     * Default configuration path.
     */
    public static final Path DEFAULT_CONFIG = Paths.get("heroic.yml");

    private static final Map<String, HeroicProfile> PROFILES = new HashMap<>();

    static {
        PROFILES.put("generated", new GeneratedProfile());
    }

    // @formatter:off
    private static final List<Class<?>> MODULES = ImmutableList.<Class<?>>of(
        com.spotify.heroic.metric.astyanax.Entry.class,
        com.spotify.heroic.metric.datastax.Entry.class,
        com.spotify.heroic.metric.generated.Entry.class,

        com.spotify.heroic.metadata.elasticsearch.Entry.class,
        com.spotify.heroic.suggest.elasticsearch.Entry.class,
        // com.spotify.heroic.suggest.lucene.Entry.class,

        com.spotify.heroic.cluster.discovery.simple.Entry.class,

        com.spotify.heroic.aggregation.simple.Entry.class,

        com.spotify.heroic.consumer.kafka.Entry.class,

        com.spotify.heroic.aggregationcache.cassandra2.Entry.class,

        com.spotify.heroic.rpc.httprpc.Entry.class,
        com.spotify.heroic.rpc.nativerpc.Entry.class
    );
    // @formatter:on

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

        if (configuration != null)
            configuration.configure(builder, params);

        final HeroicCore core = builder.build();

        try {
            core.start();
        } catch (final Exception e) {
            log.error("Failed to start heroic", e);
            System.exit(1);
            return;
        }

        final Thread shutdown = new Thread() {
            @Override
            public void run() {
                core.shutdown();
            }
        };

        shutdown.setName("heroic-shutdown-hook");

        /* setup shutdown hook */
        Runtime.getRuntime().addShutdownHook(shutdown);

        /* block until core stops */
        core.join();
        System.exit(0);
    }

    private static void configureBuilder(final HeroicCore.Builder builder, final Parameters params) {
        final Path config = getConfigPath(params.extra);

        builder.server(true);

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

        if (params.profile != null)
            builder.profile(setupProfile(params.profile));

        builder.modules(MODULES);
    }

    private static HeroicProfile setupProfile(final String profile) {
        final HeroicProfile p = PROFILES.get(profile);

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
            log.error("Argument error", e);
            return null;
        }

        if (params.help) {
            parser.printUsage(System.out);

            System.out.println("Available Profiles (activate with: -p <profile>):");

            for (final Map.Entry<String, HeroicProfile> entry : PROFILES.entrySet()) {
                System.out.println("  " + entry.getKey() + " - " + entry.getValue().description());
            }

            return null;
        }

        return params;
    }

    @ToString
    @Data
    public static class Parameters {
        @Option(name = "-P", aliases = { "--profile" }, usage = "Activate a pre-defined profile instead of a configuration file. Profiles are pre-defined configurations, useful for messing around with the system.")
        private String profile;

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

        @Argument
        private final List<String> extra = new ArrayList<>();
    }
}
