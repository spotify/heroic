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
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.http.StoredMetricsQueries;
import com.spotify.heroic.injection.Startable;
import com.spotify.heroic.injection.Stoppable;
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

@Slf4j
public class Main extends GuiceServletContextListener {
    public static final String DEFAULT_CONFIG = "heroic.yml";

    public static Injector injector;

    public static List<Startable> startable = new ArrayList<Startable>();
    public static List<Stoppable> stoppable = new ArrayList<Stoppable>();

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

    public static Injector setupInjector(final HeroicConfig config, final HeroicReporter reporter) {
        log.info("Building Guice Injector");

        final List<Module> modules = new ArrayList<Module>();
        final StoredMetricsQueries storedMetricsQueries = new StoredMetricsQueries();
        final AggregationCache cache = config.getAggregationCache();

        modules.add(new AbstractModule() {
            @Override
            protected void configure() {
                if (cache == null) {
                    bind(AggregationCache.class).toProvider(
                            Providers.of((AggregationCache) null));
                } else {
                    bind(AggregationCache.class).toInstance(cache);
                }

                bind(MetricBackendManager.class).toInstance(new MetricBackendManager(reporter.newMetricBackendManager(null), config.getMaxAggregationMagnitude()));
                bind(MetadataBackendManager.class).toInstance(new MetadataBackendManager(reporter.newMetadataBackendManager(null)));
                bind(StoredMetricsQueries.class).toInstance(storedMetricsQueries);

                {
                    final Multibinder<MetricBackend> bindings = Multibinder.newSetBinder(binder(), MetricBackend.class);
                    for (final MetricBackend backend : config.getMetricBackends()) {
                        bindings.addBinding().toInstance(backend);
                    }
                }

                {
                    final Multibinder<MetadataBackend> bindings = Multibinder.newSetBinder(binder(), MetadataBackend.class);
                    for (final MetadataBackend backend : config.getMetadataBackends()) {
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

                bindListener(new IsSubclassOf(Startable.class), new TypeListener() {
                    @Override
                    public <I> void hear(TypeLiteral<I> type,
                            TypeEncounter<I> encounter) {
                        encounter.register(new InjectionListener<I>() {
                            @Override
                            public void afterInjection(Object i) {
                                startable.add((Startable) i);
                            }
                        });
                    }
                });

                bindListener(new IsSubclassOf(Stoppable.class), new TypeListener() {
                    @Override
                    public <I> void hear(TypeLiteral<I> type,
                            TypeEncounter<I> encounter) {
                        encounter.register(new InjectionListener<I>() {
                            @Override
                            public void afterInjection(Object i) {
                                stoppable.add((Stoppable) i);
                            }
                        });
                    }
                });
            }
        });

        modules.add(new SchedulerModule());

        return Guice.createInjector(modules);
    }

    public static void main(String[] args) {
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

        final FastForwardReporter ffwd = FastForwardReporter
                .forRegistry(registry).schedule(TimeUnit.SECONDS, 30)
                .prefix(MetricId.build("heroic").tagged("service", "heroic"))
                .build();
        ffwd.start();

        if (config == null) {
            log.error("No configuration, shutting down");
            System.exit(1);
            return;
        }

        injector = setupInjector(config, reporter);

        /* fire startable handlers */
        for (final Startable startable : Main.startable) {
            try {
                startable.start();
            } catch (Exception e) {
                log.error("Failed to start {}", startable, e);
                System.exit(1);
            }
        }

        final GrizzlyServer grizzlyServer = new GrizzlyServer();
        final HttpServer server;

        final URI baseUri = UriBuilder.fromUri("http://0.0.0.0/").port(8080)
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

                log.warn("Bye Bye!");
                latch.countDown();
            }
        });

        Runtime.getRuntime().addShutdownHook(hook);

        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Shutdown interrupted", e);
        }

        /* fire Stoppable handlers */
        for (final Stoppable stoppable : Main.stoppable) {
            try {
                stoppable.stop();
            } catch (Exception e) {
                log.error("Failed to stop {}", startable, e);
            }
        }

        System.exit(0);
    }
}
