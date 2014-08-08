package com.spotify.heroic;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.UriBuilder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.glassfish.grizzly.http.server.HttpServer;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.AbstractMatcher;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import com.google.inject.util.Providers;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.http.StoredMetricQueries;
import com.spotify.heroic.injection.Delegator;
import com.spotify.heroic.injection.Lifecycle;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.MetricBackend;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.semantic.SemanticHeroicReporter;
import com.spotify.heroic.yaml.HeroicConfig;
import com.spotify.heroic.yaml.ValidationException;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.metrics.ffwd.FastForwardReporter;
import com.spotify.metrics.jvm.GarbageCollectorMetricSet;
import com.spotify.metrics.jvm.ThreadStatesMetricSet;

@Slf4j
public class Main extends GuiceServletContextListener {
    public static final String DEFAULT_CONFIG = "heroic.yml";

    public static Injector injector;

    public static List<Lifecycle> managed = new ArrayList<Lifecycle>();

    @Override
    protected Injector getInjector() {
        return injector;
    }

    @RequiredArgsConstructor
    private static class IsSubclassOf extends AbstractMatcher<TypeLiteral<?>> {
        private final Class<?> clazz;

        @Override
        public boolean matches(TypeLiteral<?> t) {
            return clazz.isAssignableFrom(t.getRawType());
        }
    }

    public static Injector setupInjector(final HeroicConfig config,
            final HeroicReporter reporter) {
        log.info("Building Guice Injector");

        final List<Module> modules = new ArrayList<Module>();
        final StoredMetricQueries storedMetricsQueries = new StoredMetricQueries();
        final AggregationCache cache = config.getAggregationCache();

        final List<MetricBackend> metricBackends = config.getMetricBackends();
        final List<MetadataBackend> metadataBackends = config
                .getMetadataBackends();

        final MetricBackendManager metric = new MetricBackendManager(
                reporter.newMetricBackendManager(), metricBackends,
                config.getMaxAggregationMagnitude(), config.isUpdateMetadata());

        final MetadataBackendManager metadata = new MetadataBackendManager(
                reporter.newMetadataBackendManager(), metadataBackends);

        final ClusterManager cluster = config.getCluster();

        modules.add(new AbstractModule() {
            @Override
            protected void configure() {
                if (cache == null) {
                    bind(AggregationCache.class).toProvider(
                            Providers.of((AggregationCache) null));
                } else {
                    bind(AggregationCache.class).toInstance(cache);
                }

                bind(MetricBackendManager.class).toInstance(metric);
                bind(MetadataBackendManager.class).toInstance(metadata);
                bind(StoredMetricQueries.class).toInstance(storedMetricsQueries);
                bind(ClusterManager.class).toInstance(cluster);

                setupBackends(MetricBackend.class, metricBackends);

                {
                    final Multibinder<MetadataBackend> bindings = Multibinder
                            .newSetBinder(binder(), MetadataBackend.class);
                    for (final MetadataBackend backend : metadataBackends) {
                        bindings.addBinding().toInstance(backend);
                    }
                }

                {
                    final Multibinder<Consumer> bindings = Multibinder
                            .newSetBinder(binder(), Consumer.class);
                    for (final Consumer consumer : config.getConsumers()) {
                        bindings.addBinding().toInstance(consumer);
                    }
                }

                bindListener(new IsSubclassOf(Lifecycle.class),
                        new TypeListener() {
                    @Override
                    public <I> void hear(TypeLiteral<I> type,
                            TypeEncounter<I> encounter) {
                        encounter.register(new InjectionListener<I>() {
                            @Override
                            public void afterInjection(Object i) {
                                managed.add((Lifecycle) i);
                            }
                        });
                    }
                });
            }

            @SuppressWarnings("unchecked")
            private <T> void setupBackends(Class<T> clazz, List<T> backends) {
                final Multibinder<T> bindings = Multibinder.newSetBinder(
                        binder(), clazz);
                for (T backend : backends) {
                    bindings.addBinding().toInstance(backend);

                    while (backend instanceof Delegator) {
                        backend = ((Delegator<T>) backend).delegate();
                        bindings.addBinding().toInstance(backend);
                    }
                }
            }
        });

        modules.add(new SchedulerModule());

        return Guice.createInjector(modules);
    }

    public static void main(String[] args) throws IOException {
        final String configPath;

        if (args.length < 1) {
            configPath = DEFAULT_CONFIG;
        } else {
            configPath = args[0];
        }

        final HeroicConfig config;

        final SemanticMetricRegistry registry = new SemanticMetricRegistry();
        final HeroicReporter reporter = new SemanticHeroicReporter(registry);

        log.info("Loading configuration from: {}", configPath);
        try {
            config = HeroicConfig.parse(Paths.get(configPath), reporter);
        } catch (ValidationException | IOException e) {
            log.error("Invalid configuration file: " + configPath, e);
            System.exit(1);
            return;
        }

        final FastForwardReporter ffwd = setupReporter(registry);

        if (config == null) {
            log.error("No configuration, shutting down");
            System.exit(1);
            return;
        }

        injector = setupInjector(config, reporter);

        /* fire startable handlers */
        for (final Lifecycle startable : Main.managed) {
            try {
                startable.start();
            } catch (final Exception e) {
                log.error("Failed to start {}", startable, e);
                System.exit(1);
            }
        }

        final GrizzlyServer grizzlyServer = new GrizzlyServer();
        final HttpServer server;

        final URI baseUri = UriBuilder.fromUri("http://0.0.0.0/")
                .port(config.getPort())
                .build();

        try {
            server = grizzlyServer.start(baseUri);
        } catch (final IOException e) {
            log.error("Failed to start grizzly server", e);
            System.exit(1);
            return;
        }

        final Scheduler scheduler = injector.getInstance(Scheduler.class);

        try {
            scheduler.triggerJob(SchedulerModule.REFRESH_TAGS);
        } catch (final SchedulerException e) {
            log.error("Failed to schedule initial tags refresh", e);
            System.exit(1);
            return;
        }

        try {
            scheduler.triggerJob(SchedulerModule.REFRESH_CLUSTER);
        } catch (final SchedulerException e) {
            log.error("Failed to schedule initial cluster refresh", e);
            System.exit(1);
            return;
        }

        final CountDownLatch latch = new CountDownLatch(1);

        final Thread hook = new Thread(new Runnable() {
            @Override
            public void run() {
                log.warn("Shutting down scheduler");

                try {
                    scheduler.shutdown(true);
                } catch (final SchedulerException e) {
                    log.error("Scheduler shutdown failed", e);
                }

                try {
                    log.warn("Waiting for server to shutdown");
                    server.shutdown().get(30, TimeUnit.SECONDS);
                } catch (final Exception e) {
                    log.error("Server shutdown failed", e);
                }

                log.info("Firing stop handles");

                /* fire Stoppable handlers */
                for (final Lifecycle stoppable : Main.managed) {
                    try {
                        stoppable.stop();
                    } catch (final Exception e) {
                        log.error("Failed to stop {}", stoppable, e);
                    }
                }

                ffwd.stop();

                log.warn("Bye Bye!");
                latch.countDown();
            }
        });

        Runtime.getRuntime().addShutdownHook(hook);

        /*
        System.out.println("Enter to exit...");
        System.in.read();

        hook.start();
         */

        try {
            latch.await();
        } catch (final InterruptedException e) {
            log.error("Shutdown interrupted", e);
        }

        System.exit(0);
    }

    private static FastForwardReporter setupReporter(final SemanticMetricRegistry registry) throws IOException {
        final MetricId gauges = MetricId.build();

        registry.register(gauges, new ThreadStatesMetricSet());
        registry.register(gauges, new GarbageCollectorMetricSet());
        registry.register(gauges, new MemoryUsageGaugeSet());

        final FastForwardReporter ffwd = FastForwardReporter
                .forRegistry(registry).schedule(TimeUnit.SECONDS, 30)
                .prefix(MetricId.build("heroic").tagged("service", "heroic")).build();

        ffwd.start();

        return ffwd;
    }
}
