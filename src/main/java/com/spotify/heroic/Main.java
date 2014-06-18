package com.spotify.heroic;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.ws.rs.core.UriBuilder;

import lombok.extern.slf4j.Slf4j;

import org.glassfish.grizzly.http.server.HttpServer;
import org.quartz.Scheduler;
import org.quartz.SchedulerException;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.servlet.GuiceServletContextListener;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.http.StoredMetricsQueries;
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

    static Injector injector;

    @Override
    protected Injector getInjector() {
        return injector;
    }

    public static Injector setupInjector(final HeroicConfig config, final HeroicReporter reporter) {
        log.info("Building Guice Injector");

        final List<Module> modules = new ArrayList<Module>();
        final StoredMetricsQueries storedMetricsQueries = new StoredMetricsQueries();

        modules.add(new AbstractModule() {
            @Override
            protected void configure() {
                bind(AggregationCache.class).toInstance(config.getAggregationCache());
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

        try {
            config = HeroicConfig.parse(Paths.get(configPath), reporter);
        } catch (ValidationException | IOException e) {
            log.error("Invalid configuration file: " + configPath);
            System.exit(1);
            return;
        }

        final FastForwardReporter ffwd = FastForwardReporter.forRegistry(registry)
                .schedule(TimeUnit.MINUTES, 5).prefix(MetricId.build("heroic").tagged("service", "heroic")).build();
        ffwd.start();

        if (config == null) {
            log.error("No configuration, shutting down");
            System.exit(1);
            return;
        }

        injector = setupInjector(config, reporter);

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

        System.exit(0);
    }
}
