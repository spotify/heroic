package com.spotify.heroic;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewritePatternRule;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.Slf4jRequestLog;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.servlet.ServletContainer;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerConfig;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.semantic.SemanticHeroicReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.metrics.ffwd.FastForwardReporter;
import com.spotify.metrics.jvm.GarbageCollectorMetricSet;
import com.spotify.metrics.jvm.MemoryUsageGaugeSet;
import com.spotify.metrics.jvm.ThreadStatesMetricSet;

@Slf4j
public class HeroicService {
    /**
     * Which resource file to load modules from.
     */
    private static final String HEROIC_MODULES = "heroic.modules";

    /**
     * Default configuration path.
     */
    public static final String DEFAULT_CONFIG = "heroic.yml";

    public static Set<LifeCycle> lifecycles = new HashSet<>();

    public static Injector setupInjector(final HeroicConfig config, final HeroicReporter reporter,
            final ScheduledExecutorService scheduledExecutor, final HeroicLifeCycle lifecycle) throws Exception {
        log.info("Building Guice Injector");

        final List<Module> modules = new ArrayList<Module>();

        final ObjectMapper mapper = new ObjectMapper();

        modules.add(new HeroicModule(lifecycle, scheduledExecutor, lifecycles, reporter, mapper));
        modules.add(new HeroicSchedulerModule(config.getRefreshClusterSchedule()));
        modules.add(config.getHttpClientManagerModule());
        modules.add(config.getMetricModule());
        modules.add(config.getMetadataModule());
        modules.add(config.getClusterManagerModule());
        modules.add(config.getAggregationCacheModule());
        modules.add(config.getIngestionModule());

        modules.addAll(setupConsumers(config, reporter));

        final Injector injector = Guice.createInjector(modules);

        // touch all bindings to make sure they are 'eagerly' initialized.
        for (final Entry<Key<?>, Binding<?>> entry : injector.getAllBindings().entrySet()) {
            entry.getValue().getProvider().get();
        }

        return injector;
    }

    private static List<Module> setupConsumers(final HeroicConfig config, final HeroicReporter reporter) {
        final List<Module> modules = new ArrayList<>();

        int consumerCount = 0;

        for (final ConsumerConfig consumer : config.getConsumers()) {
            final String id = consumer.id() != null ? consumer.id() : consumer.buildId(consumerCount++);
            final Key<Consumer> key = Key.get(Consumer.class, Names.named(id));
            modules.add(consumer.module(key, reporter.newConsumer(id)));
        }

        return modules;
    }

    public static void main(String[] args) throws Exception {
        final String configPath;
        final String modulesPath;

        if (args.length > 0) {
            configPath = args[0];
        } else {
            configPath = DEFAULT_CONFIG;
        }

        if (args.length > 1) {
            modulesPath = args[1];
        } else {
            modulesPath = null;
        }

        final SemanticMetricRegistry registry = new SemanticMetricRegistry();
        final HeroicReporter reporter = new SemanticHeroicReporter(registry);

        final HeroicConfig config = setupConfig(configPath, modulesPath, reporter);

        if (config == null) {
            System.exit(1);
            return;
        }

        final ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(10);

        final CountDownLatch startupLatch = new CountDownLatch(1);

        final HeroicLifeCycle lifecycle = new HeroicLifeCycle() {
            @Override
            public void awaitStartup() throws InterruptedException {
                startupLatch.await();
            }
        };

        final Injector injector = setupInjector(config, reporter, scheduledExecutor, lifecycle);

        final Server server = setupHttpServer(config, injector);
        final FastForwardReporter ffwd = setupReporter(registry);

        try {
            server.start();
        } catch (final Exception e) {
            log.error("Failed to start server", e);
            System.exit(1);
            return;
        }

        /* fire startable handlers */
        if (!startLifeCycles()) {
            log.info("Failed to start all lifecycle components");
            System.exit(1);
            return;
        }

        final Scheduler scheduler = injector.getInstance(Scheduler.class);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(setupShutdownHook(ffwd, server, scheduler, latch, scheduledExecutor));

        startupLatch.countDown();
        log.info("Heroic was successfully started!");

        latch.await();
        System.exit(0);
    }

    private static boolean startLifeCycles() {
        boolean ok = true;

        /* fire Stoppable handlers */
        for (final LifeCycle startable : lifecycles) {
            log.info("Starting: {}", startable);

            try {
                startable.start();
            } catch (final Exception e) {
                log.error("Failed to start {}", startable, e);
                ok = false;
            }
        }

        return ok;
    }

    private static boolean stopLifeCycles() {
        boolean ok = true;

        /* fire Stoppable handlers */
        for (final LifeCycle stoppable : lifecycles) {
            log.info("Stopping: {}", stoppable);

            try {
                stoppable.stop();
            } catch (final Exception e) {
                log.error("Failed to stop {}", stoppable, e);
                ok = false;
            }
        }

        return ok;
    }

    private static HeroicConfig setupConfig(final String configPath, final String modulesPath,
            final HeroicReporter reporter) throws IOException {
        log.info("Loading configuration from: {}", configPath);

        final HeroicConfig config;
        final Path path = Paths.get(configPath);

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final HeroicContext context = new HeroicContext() {
            @Override
            public void register(String name, Class<?> type) {
                mapper.registerSubtypes(new NamedType(type, name));
            }
        };

        {
            final List<URL> moduleLocations = new ArrayList<>();

            if (modulesPath != null) {
                moduleLocations.add(new File(modulesPath).toURI().toURL());
            }

            final ClassLoader loader = HeroicService.class.getClassLoader();
            moduleLocations.add(loader.getResource(HEROIC_MODULES));

            List<HeroicModuleEntryPoint> modules = ModuleUtils.loadModules(moduleLocations);

            for (final HeroicModuleEntryPoint entry : modules) {
                entry.setup(context);
            }
        }

        try {
            config = mapper.readValue(Files.newInputStream(path), HeroicConfig.class);
        } catch (final JsonMappingException e) {
            final JsonLocation location = e.getLocation();
            log.error(String.format("%s[%d:%d]: %s", configPath, location == null ? null : location.getLineNr(),
                    location == null ? null : location.getColumnNr(), e.getOriginalMessage()));

            if (log.isDebugEnabled())
                log.debug("Configuration error", e);

            return null;
        }

        return config;
    }

    private static Server setupHttpServer(final HeroicConfig config, Injector injector) throws IOException {
        log.info("Starting HTTP Server...");

        // statically provide injector to jersey application.
        HeroicJerseyApplication.setInjector(injector);

        final Server server = new Server(config.getPort());

        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");

        // Initialize and register Jersey ServletContainer
        final ServletHolder jerseyServlet = context.addServlet(ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(1);
        jerseyServlet.setInitParameter("javax.ws.rs.Application", HeroicJerseyApplication.class.getName());

        final RequestLogHandler requestLogHandler = new RequestLogHandler();

        requestLogHandler.setRequestLog(new Slf4jRequestLog());

        final RewriteHandler rewrite = new RewriteHandler();
        makeRewriteRules(rewrite);

        final HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] { rewrite, context, requestLogHandler });

        server.setHandler(handlers);

        return server;
    }

    private static void makeRewriteRules(RewriteHandler rewrite) {
        {
            final RewritePatternRule rule = new RewritePatternRule();
            rule.setPattern("/metrics");
            rule.setReplacement("/query/metrics");
            rewrite.addRule(rule);
        }

        {
            final RewritePatternRule rule = new RewritePatternRule();
            rule.setPattern("/metrics-stream/*");
            rule.setReplacement("/query/metrics-stream");
            rewrite.addRule(rule);
        }

        {
            final RewritePatternRule rule = new RewritePatternRule();
            rule.setPattern("/tags");
            rule.setReplacement("/metadata/tags");
            rewrite.addRule(rule);
        }

        {
            final RewritePatternRule rule = new RewritePatternRule();
            rule.setPattern("/keys");
            rule.setReplacement("/metadata/keys");
            rewrite.addRule(rule);
        }
    }

    private static FastForwardReporter setupReporter(final SemanticMetricRegistry registry) throws IOException {
        final MetricId gauges = MetricId.build();

        registry.register(gauges, new ThreadStatesMetricSet());
        registry.register(gauges, new GarbageCollectorMetricSet());
        registry.register(gauges, new MemoryUsageGaugeSet());

        final FastForwardReporter ffwd = FastForwardReporter.forRegistry(registry).schedule(TimeUnit.SECONDS, 30)
                .prefix(MetricId.build("heroic").tagged("service", "heroic")).build();

        ffwd.start();

        return ffwd;
    }

    private static Thread setupShutdownHook(final FastForwardReporter ffwd, final Server server,
            final Scheduler scheduler, final CountDownLatch latch, final ScheduledExecutorService scheduledExecutor) {
        return new Thread() {
            @Override
            public void run() {
                log.info("Shutting down Heroic");

                log.info("Shutting down scheduler");

                try {
                    scheduler.shutdown(true);
                } catch (final SchedulerException e) {
                    log.error("Scheduler shutdown failed", e);
                }

                try {
                    log.info("Waiting for server to shutdown");
                    server.stop();
                    server.join();
                } catch (final Exception e) {
                    log.error("Server shutdown failed", e);
                }

                log.info("Stopping scheduled executor service");

                scheduledExecutor.shutdownNow();

                try {
                    scheduledExecutor.awaitTermination(30, TimeUnit.SECONDS);
                } catch (final InterruptedException e) {
                    log.error("Failed to shut down scheduled executor service");
                }

                log.info("Stopping life cycles");
                stopLifeCycles();

                log.info("Stopping fast forward reporter");
                ffwd.stop();

                if (LogManager.getContext() instanceof LoggerContext) {
                    log.info("Shutting down log4j2, Bye Bye!");
                    Configurator.shutdown((LoggerContext) LogManager.getContext());
                } else {
                    log.warn("Unable to shutdown log4j2, Bye Bye!");
                }

                latch.countDown();
            }
        };
    }
}
