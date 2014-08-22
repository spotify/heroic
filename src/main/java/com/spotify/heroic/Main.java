package com.spotify.heroic;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.servlet.DispatcherType;
import javax.ws.rs.core.UriBuilder;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.grizzly.servlet.FilterRegistration;
import org.glassfish.grizzly.servlet.ServletRegistration;
import org.glassfish.grizzly.servlet.WebappContext;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.servlet.ServletContainer;
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
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.google.inject.spi.InjectionListener;
import com.google.inject.spi.TypeEncounter;
import com.google.inject.spi.TypeListener;
import com.google.inject.util.Providers;
import com.spotify.heroic.cache.AggregationCache;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.http.StoredMetricQueries;
import com.spotify.heroic.injection.Lifecycle;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataBackendManager;
import com.spotify.heroic.metrics.Backend;
import com.spotify.heroic.metrics.MetricBackendManager;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.semantic.SemanticHeroicReporter;
import com.spotify.heroic.yaml.HeroicConfig;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import com.spotify.metrics.ffwd.FastForwardReporter;
import com.spotify.metrics.jvm.GarbageCollectorMetricSet;
import com.spotify.metrics.jvm.ThreadStatesMetricSet;

@Slf4j
public class Main {
	public static final String DEFAULT_CONFIG = "heroic.yml";

	public static Injector injector;

	public static List<Lifecycle> managed = new ArrayList<Lifecycle>();

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
			final HeroicReporter reporter) {
		log.info("Building Guice Injector");

		final List<Module> modules = new ArrayList<Module>();
		final StoredMetricQueries storedMetricsQueries = new StoredMetricQueries();
		final AggregationCache cache = config.getAggregationCache();

		final MetricBackendManager metric = new MetricBackendManager(
				reporter.newMetricBackendManager(), config.getMetricBackends(),
				config.isUpdateMetadata(), config.getGroupLimit(),
				config.getGroupLoadLimit());

		final MetadataBackendManager metadata = new MetadataBackendManager(
				reporter.newMetadataBackendManager(),
				config.getMetadataBackends());

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

				bind(ClusterManager.class).toInstance(cluster);
				bind(MetricBackendManager.class).toInstance(metric);
				bind(MetadataBackendManager.class).toInstance(metadata);
				bind(StoredMetricQueries.class)
				.toInstance(storedMetricsQueries);
				bind(ClusterManager.class).toInstance(cluster);

				multiBind(config.getMetricBackends(), Backend.class);
				multiBind(config.getMetadataBackends(), MetadataBackend.class);
				multiBind(config.getConsumers(), Consumer.class);

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

		final HeroicConfig config = setupConfig(configPath, reporter);

		injector = setupInjector(config, reporter);

		/* fire startable handlers */
		if (!startLifecycle()) {
			log.info("Failed to start all lifecycle components");
			System.exit(1);
			return;
		}

		final HttpServer server = setupHttpServer(config);
		final FastForwardReporter ffwd = setupReporter(registry);

		server.start();

		final Scheduler scheduler = injector.getInstance(Scheduler.class);
		scheduler.triggerJob(SchedulerModule.REFRESH_CLUSTER);

		final CountDownLatch latch = new CountDownLatch(1);

		Runtime.getRuntime().addShutdownHook(
				new Thread(setupShutdownHook(ffwd, server, scheduler, latch)));

		latch.await();
		System.exit(0);
	}

	/**
	 * Start the lifecycle of all managed components.
	 */
	private static boolean startLifecycle() {
		boolean ok = true;

		for (final Lifecycle startable : Main.managed) {
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

	private static boolean stopLifecycle() {
		boolean ok = true;

		/* fire Stoppable handlers */
		for (final Lifecycle stoppable : Main.managed) {
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
			final HeroicReporter reporter) throws Exception {
		log.info("Loading configuration from: {}", configPath);

		final HeroicConfig config = HeroicConfig.parse(Paths.get(configPath),
				reporter);

		if (config == null) {
			throw new Exception(
					"INTERNAL ERROR: No configuration, shutting down");
		}

		return config;
	}

	private static HttpServer setupHttpServer(final HeroicConfig config)
			throws IOException {
		log.info("Starting grizzly http server...");

		final URI baseUri = UriBuilder.fromUri("http://0.0.0.0/")
				.port(config.getPort()).build();

		final WebappContext context = new WebappContext("Guice Webapp sample",
				"");

		context.addListener(Main.LISTENER);
		context.addFilter(GuiceFilter.class.getName(), GuiceFilter.class)
		.addMappingForUrlPatterns(null, "/*");

		// Initialize and register Jersey ServletContainer
		final ServletRegistration servletRegistration = context.addServlet(
				"ServletContainer", ServletContainer.class);
		servletRegistration.addMapping("/*");
		servletRegistration.setInitParameter("javax.ws.rs.Application",
				WebApp.class.getName());

		// Initialize and register GuiceFilter
		final FilterRegistration registration = context.addFilter(
				"GuiceFilter", GuiceFilter.class);
		registration.addMappingForUrlPatterns(
				EnumSet.allOf(DispatcherType.class), "/*");

		final HttpServer server = GrizzlyHttpServerFactory.createHttpServer(
				baseUri, false);

		context.deploy(server);

		return server;
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

	private static Runnable setupShutdownHook(final FastForwardReporter ffwd,
			final HttpServer server, final Scheduler scheduler,
			final CountDownLatch latch) {
		return new Runnable() {
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

				stopLifecycle();

				ffwd.stop();

				log.warn("Bye Bye!");
				latch.countDown();
			}
		};
	}
}
