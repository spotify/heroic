package com.spotify.heroic.coalesce;

import java.io.File;
import java.io.PrintWriter;
import java.util.concurrent.CountDownLatch;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.Inject;
import com.spotify.heroic.HeroicBootstrap;
import com.spotify.heroic.HeroicCore;
import com.spotify.heroic.HeroicInternalLifeCycle;
import com.spotify.heroic.HeroicModules;
import com.spotify.heroic.QueryManager;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CoalesceService {
    public static void main(String argv[]) {
        final Parameters params = new Parameters();

        final CmdLineParser parser = new CmdLineParser(params);

        try {
            parser.parseArgument(argv);
        } catch (CmdLineException e) {
            log.error("Argument error", e);
            System.exit(1);
            return;
        }

        if (params.help) {
            parser.printUsage(System.err);
            HeroicModules.printProfileUsage(new PrintWriter(System.err), "-P");
            System.exit(0);
            return;
        }

        final CountDownLatch latch = new CountDownLatch(1);

        final HeroicBootstrap bootstrap = new HeroicBootstrap() {
            @Inject
            HeroicInternalLifeCycle lifecycle;

            @Override
            public void run() throws Exception {
                lifecycle.registerShutdown("coalesce", latch::countDown);
            }
        };

        /* create a core which does not attempt to use local backends */
        final HeroicCore core = HeroicCore.builder().disableBackends(true).setupServer(false).modules(HeroicModules.ALL_MODULES)
                .configPath(params.heroicConfig).bootstrap(bootstrap).build();

        try {
            core.start();
        } catch (Exception e) {
            log.error("Failed to start heroic core", e);
            System.exit(1);
            return;
        }

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final CoalesceConfig config;

        try {
            config = mapper.readValue(new File(params.config), CoalesceConfig.class);
        } catch (Exception e) {
            log.error("Failed to read configuration: {}", params.config, e);
            System.exit(1);
            return;
        }

        try {
            core.injectInstance(CoalesceService.class).run(latch, config);
        } catch (Exception e) {
            log.error("Failed to run coalesce service", e);
        }

        try {
            core.shutdown();
        } catch (Exception e) {
            log.error("Failed to shutdown heroic core", e);
            System.exit(1);
            return;
        }

        System.exit(0);
    }

    private final QueryManager query;

    @Inject
    public CoalesceService(final QueryManager query) {
        this.query = query;
    }

    public void run(CountDownLatch latch, CoalesceConfig config) throws Exception {
        log.info("Waiting for query engine to initialize...");
        query.initialized().get();

        /* do something */
    }

    @ToString
    private static class Parameters {
        @Option(name = "--config", required = true, usage = "Use coalesce configuration path")
        private String config = null;

        @Option(name = "--heroic-config", required = true, usage = "Use heroic configuration path")
        private String heroicConfig = null;

        @Option(name = "-h", aliases = { "--help" }, help = true, usage = "Display help")
        private boolean help = false;
    }
}