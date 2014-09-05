package com.spotify.heroic;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.servlet.DispatcherType;

import lombok.RequiredArgsConstructor;
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
import org.yaml.snakeyaml.error.YAMLException;

import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.AbstractMatcher;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cache.AggregationCacheBackend;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.LocalClusterNode;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.http.query.QueryResource.StoredMetricQueries;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.migrator.SeriesMigrator;
import com.spotify.heroic.statistics.AggregationCacheBackendReporter;
import com.spotify.heroic.statistics.AggregationCacheReporter;
import com.spotify.heroic.statistics.BackendReporter;
import com.spotify.heroic.statistics.ConsumerReporter;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.MetadataBackendManagerReporter;
import com.spotify.heroic.statistics.MetadataBackendReporter;
import com.spotify.heroic.statistics.MetricBackendManagerReporter;
import com.spotify.heroic.statistics.semantic.SemanticHeroicReporter;
import com.spotify.heroic.yaml.HeroicConfig;
import com.spotify.heroic.yaml.ValidationException;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.metrics.ffwd.FastForwardReporter;
import com.spotify.metrics.jvm.GarbageCollectorMetricSet;
import com.spotify.metrics.jvm.ThreadStatesMetricSet;

@Slf4j
public class Main {
    public static final String DEFAULT_CONFIG = "heroic.yml";

    public static Injector injector;

    public static List<LifeCycle> managed = new ArrayList<LifeCycle>();

    public static final GuiceServletContextListener LISTENER = new GuiceServletContextListener() {
        @Override
        protected Injector getInjector() {
            return injector;
        }
    };

    @RequiredArgsConstructor
    private static class IsSubclassOf extends AbstractMatcher<TypeLiteral<?>> {
        private final Class<?> clazz;

        @Override
        public boolean matches(TypeLiteral<?> t) {
            return clazz.isAssignableFrom(t.getRawType());
        }
    }

    public static Injector setupInjector(final HeroicConfig config,
            final HeroicReporter reporter,
            final ScheduledExecutorService scheduledExecutor,
            final ApplicationLifecycle lifecycle) {
        log.info("Building Guice Injector");

        final List<Module> modules = new ArrayList<Module>();
        final StoredMetricQueries storedMetricsQueries = new StoredMetricQueries();
        final AggregationCache cache = config.getCache();

        final MetricBackendManager metrics = config.getMetrics();
        final MetadataBackendManager metadata = config.getMetadata();
        final ClusterManager cluster = config.getCluster();
        final SeriesMigrator migrator = new SeriesMigrator();

        final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<Runnable>(
                100000);
        final ThreadPoolExecutor executor = new ThreadPoolExecutor(50, 100, 60,
                TimeUnit.SECONDS, queue);

        modules.add(new AbstractModule() {
            @Provides
            public ConsumerReporter newConsumerReporter() {
                return reporter.newConsumer();
            }

            @Provides
            public MetadataBackendManagerReporter newMetadataBackendManager() {
                return reporter.newMetadataBackendManager();
            }

            @Provides
            public BackendReporter newBackend() {
                return reporter.newBackend();
            }

            @Provides
            public MetricBackendManagerReporter newMetricBackendManager() {
                return reporter.newMetricBackendManager();
            }

            @Provides
            public MetadataBackendReporter newMetadataBackend() {
                return reporter.newMetadataBackend();
            }

            @Provides
            public AggregationCacheReporter newAggregationCache() {
                return reporter.newAggregationCache();
            }

            @Provides
            public AggregationCacheBackendReporter newAggregationCacheBackend() {
                return reporter.newAggregationCacheBackend();
            }

            @Override
            protected void configure() {
                bind(ApplicationLifecycle.class).toInstance(lifecycle);
                bind(SeriesMigrator.class).toInstance(migrator);
                bind(ScheduledExecutorService.class).toInstance(
                        scheduledExecutor);
                bind(ExecutorService.class).toInstance(executor);
                bind(AggregationCache.class).toInstance(cache);
                bind(ClusterManager.class).toInstance(cluster);
                bind(MetricBackendManager.class).toInstance(metrics);
                bind(MetadataBackendManager.class).toInstance(metadata);
                bind(StoredMetricQueries.class)
                .toInstance(storedMetricsQueries);
                bind(ClusterManager.class).toInstance(cluster);
                bind(LocalClusterNode.class).toInstance(
                        cluster.getLocalClusterNode());

                bind(AggregationCacheBackend.class).toInstance(
                        cache.getBackend());
                multiBind(metrics.getBackends(), Backend.class);
                multiBind(metadata.getBackends(), MetadataBackend.class);
                multiBind(config.getConsumers(), Consumer.class);

                bindListener(new IsSubclassOf(LifeCycle.class),
                        new TypeListener() {
                    @Override
                    public <I> void hear(TypeLiteral<I> type,
                            TypeEncounter<I> encounter) {
                        encounter.register(new InjectionListener<I>() {
                            @Override
                            public void afterInjection(Object i) {
                                managed.add((LifeCycle) i);
                            }
                        });
                    }
                });
            }

            private <T> void multiBind(final List<T> binds, Class<T> clazz) {
                {
                    final Multibinder<T> bindings = Multibinder.newSetBinder(
                            binder(), clazz);
                    for (final T backend : binds) {
                        bindings.addBinding().toInstance(backend);
                    }
                }
            }
        });

        modules.add(new SchedulerModule(config.getRefreshClusterSchedule()));

        return Guice.createInjector(modules);
    }

    public static void main(String[] args) throws Exception {
        final String configPath;

        if (args.length < 1) {
            configPath = DEFAULT_CONFIG;
        } else {
            configPath = args[0];
        }

        final SemanticMetricRegistry registry = new SemanticMetricRegistry();
        final HeroicReporter reporter = new SemanticHeroicReporter(registry);

        final HeroicConfig config;

        try {
            config = setupConfig(configPath, reporter);
        } catch (final YAMLException e) {
            log.error("Error in configuration file: {}", configPath, e);
            System.exit(1);
            return;
        } catch (final ValidationException e) {
            log.error(String.format("Error in configuration file: %s",
                    configPath), e);
            System.exit(1);
            return;
        }

        final ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(
                10);

        final CountDownLatch startupLatch = new CountDownLatch(1);

        final ApplicationLifecycle lifecycle = new ApplicationLifecycle() {
            @Override
            public void awaitStartup() throws InterruptedException {
                startupLatch.await();
            }
        };

        injector = setupInjector(config, reporter, scheduledExecutor, lifecycle);

        final Server server = setupHttpServer(config);
        final FastForwardReporter ffwd = setupReporter(registry);

        try {
            server.start();
        } catch (final Exception e) {
            log.error("Failed to start server", e);
            System.exit(1);
            return;
        }

        /* fire startable handlers */
        if (!startLifecycle()) {
            log.info("Failed to start all lifecycle components");
            System.exit(1);
            return;
        }

        final Scheduler scheduler = injector.getInstance(Scheduler.class);

        final CountDownLatch latch = new CountDownLatch(1);

        Runtime.getRuntime().addShutdownHook(
                setupShutdownHook(ffwd, server, scheduler, latch,
                        scheduledExecutor));

        startupLatch.countDown();
        log.info("Heroic was successfully started!");

        latch.await();
        System.exit(0);
    }

    /**
     * Start the lifecycle of all managed components.
     */
    private static boolean startLifecycle() {
        boolean ok = true;

        for (final LifeCycle startable : Main.managed) {
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
        for (final LifeCycle stoppable : Main.managed) {
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

    private static HeroicConfig setupConfig(final String configPath,
            final HeroicReporter reporter) throws ValidationException,
            IOException {
        log.info("Loading configuration from: {}", configPath);

        final HeroicConfig config;

        try {
            config = HeroicConfig.parse(Paths.get(configPath), reporter);
        } catch (final JsonMappingException e) {
            log.error(String.format("%s#%s: %s", configPath, e.getPath(), e));
            throw new IOException("Configuration failed", e);
        }

        if (config == null)
            throw new IOException(
                    "INTERNAL ERROR: No configuration, shutting down");

        return config;
    }

    private static Server setupHttpServer(final HeroicConfig config)
            throws IOException {
        log.info("Starting HTTP Server...");

        final Server server = new Server(config.getPort());

        final ServletContextHandler context = new ServletContextHandler(
                ServletContextHandler.NO_SESSIONS);
        context.setContextPath("/");

        // Initialize and register GuiceFilter
        context.addFilter(GuiceFilter.class, "/*",
                EnumSet.allOf(DispatcherType.class));
        context.addEventListener(Main.LISTENER);

        // Initialize and register Jersey ServletContainer
        final ServletHolder jerseyServlet = context.addServlet(
                ServletContainer.class, "/*");
        jerseyServlet.setInitOrder(1);
        jerseyServlet.setInitParameter("javax.ws.rs.Application",
                WebApp.class.getName());

        final RequestLogHandler requestLogHandler = new RequestLogHandler();

        requestLogHandler.setRequestLog(new Slf4jRequestLog());

        final RewriteHandler rewrite = new RewriteHandler();
        makeRewriteRules(rewrite);

        final HandlerCollection handlers = new HandlerCollection();
        handlers.setHandlers(new Handler[] { rewrite, context,
                requestLogHandler });
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
    }

    private static FastForwardReporter setupReporter(
            final SemanticMetricRegistry registry) throws IOException {
        final MetricId gauges = MetricId.build();

        registry.register(gauges, new ThreadStatesMetricSet());
        registry.register(gauges, new GarbageCollectorMetricSet());
        registry.register(gauges, new MemoryUsageGaugeSet());

        final FastForwardReporter ffwd = FastForwardReporter
                .forRegistry(registry).schedule(TimeUnit.SECONDS, 30)
                .prefix(MetricId.build("heroic").tagged("service", "heroic"))
                .build();

        ffwd.start();

        return ffwd;
    }

    private static Thread setupShutdownHook(final FastForwardReporter ffwd,
            final Server server, final Scheduler scheduler,
            final CountDownLatch latch,
            final ScheduledExecutorService scheduledExecutor) {
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
                    Configurator.shutdown((LoggerContext) LogManager
                            .getContext());
                } else {
                    log.warn("Unable to shutdown log4j2, Bye Bye!");
                }

                latch.countDown();
            }
        };
    }
}
