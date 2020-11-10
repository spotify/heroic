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

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lightstep.opencensus.exporter.LightStepExporterHandler;
import com.lightstep.tracer.jre.JRETracer;
import com.lightstep.tracer.shared.Options;
import com.spotify.heroic.analytics.AnalyticsComponent;
import com.spotify.heroic.cache.CacheComponent;
import com.spotify.heroic.cluster.CoreClusterComponent;
import com.spotify.heroic.cluster.DaggerCoreClusterComponent;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.Optionals;
import com.spotify.heroic.common.TypeNameMixin;
import com.spotify.heroic.consumer.ConsumersComponent;
import com.spotify.heroic.consumer.CoreConsumersModule;
import com.spotify.heroic.consumer.DaggerCoreConsumersComponent;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.dagger.CoreEarlyComponent;
import com.spotify.heroic.dagger.CoreLoadingComponent;
import com.spotify.heroic.dagger.CorePrimaryComponent;
import com.spotify.heroic.dagger.DaggerCoreComponent;
import com.spotify.heroic.dagger.DaggerCoreEarlyComponent;
import com.spotify.heroic.dagger.DaggerCoreLoadingComponent;
import com.spotify.heroic.dagger.DaggerCorePrimaryComponent;
import com.spotify.heroic.dagger.DaggerStartupPingerComponent;
import com.spotify.heroic.dagger.EarlyModule;
import com.spotify.heroic.dagger.LoadingComponent;
import com.spotify.heroic.dagger.LoadingModule;
import com.spotify.heroic.dagger.PrimaryModule;
import com.spotify.heroic.dagger.StartupPingerComponent;
import com.spotify.heroic.dagger.StartupPingerModule;
import com.spotify.heroic.generator.GeneratorComponent;
import com.spotify.heroic.http.DaggerHttpServerComponent;
import com.spotify.heroic.http.HttpServer;
import com.spotify.heroic.http.HttpServerComponent;
import com.spotify.heroic.http.HttpServerModule;
import com.spotify.heroic.ingestion.IngestionComponent;
import com.spotify.heroic.jetty.JettyConnectionFactory;
import com.spotify.heroic.lifecycle.CoreLifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleHook;
import com.spotify.heroic.lifecycle.LifeCycleNamedHook;
import com.spotify.heroic.metadata.CoreMetadataComponent;
import com.spotify.heroic.metadata.DaggerCoreMetadataComponent;
import com.spotify.heroic.metric.CoreMetricComponent;
import com.spotify.heroic.metric.DaggerCoreMetricComponent;
import com.spotify.heroic.querylogging.QueryLoggingComponent;
import com.spotify.heroic.shell.DaggerShellServerComponent;
import com.spotify.heroic.shell.ShellServerComponent;
import com.spotify.heroic.shell.ShellServerModule;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.StatisticsComponent;
import com.spotify.heroic.suggest.CoreSuggestComponent;
import com.spotify.heroic.suggest.DaggerCoreSuggestComponent;
import com.spotify.heroic.tracing.TracingConfig;
import com.spotify.heroic.usagetracking.UsageTrackingComponent;
import com.spotify.tracing.SquashingTraceExporter;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import io.opencensus.contrib.zpages.ZPageHandlers;
import io.opencensus.trace.Tracing;
import io.opencensus.trace.config.TraceConfig;
import io.opencensus.trace.samplers.Samplers;
import io.sentry.Sentry;
import java.io.IOException;
import java.io.InputStream;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Configure and bootstrap a Heroic application.
 * <p>
 * All public methods are thread-safe.
 * <p>
 * All fields are non-null.
 *
 * @author udoprog
 */
public class HeroicCore implements HeroicConfiguration {
    private static final Logger log = LoggerFactory.getLogger(HeroicCore.class);
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 8080;

    private static final boolean DEFAULT_SETUP_SERVICE = true;
    private static final boolean DEFAULT_ONESHOT = false;
    private static final boolean DEFAULT_DISABLE_BACKENDS = false;
    private static final boolean DEFAULT_SETUP_SHELL_SERVER = true;

    private static final UncaughtExceptionHandler uncaughtExceptionHandler =
        (Thread t, Throwable e) -> {
            //noinspection finally
            try {
                System.out.print("The Heroic is dying heroically now!\n");
                System.out.flush();
                System.err.println(
                    String.format("Uncaught exception caught in thread %s, exiting...", t));
                e.printStackTrace(System.err);
            } finally {
                System.exit(1);
            }
        };

    /**
     * Built-in modules that should always be loaded.
     */
    // @formatter:off
    private static final HeroicModule[] BUILTIN_MODULES = new HeroicModule[] {
        new com.spotify.heroic.aggregation.Module(),
        new com.spotify.heroic.http.Module(),
        new com.spotify.heroic.jetty.Module(),
        new com.spotify.heroic.ws.Module(),
        new com.spotify.heroic.cache.Module(),
        new com.spotify.heroic.generator.Module()
    };
    // @formatter:on

    private final Optional<String> id;
    private final Optional<String> host;
    private final Optional<Integer> port;
    private final Optional<Supplier<InputStream>> configStream;
    private final Optional<Path> configPath;
    private final Optional<URI> startupPing;
    private final Optional<String> startupId;

    /**
     * Additional dynamic parameters to pass into the configuration of a profile. These are
     * typically extracted from the commandline, or a properties file.
     */
    private final ExtraParameters params;

    /* flags */
    private final boolean setupService;
    private final boolean oneshot;
    private final boolean disableBackends;
    private final boolean setupShellServer;

    /* extensions */
    private final List<HeroicModule> modules;
    private final List<HeroicProfile> profiles;
    private final List<HeroicBootstrap> early;
    private final List<HeroicBootstrap> late;

    private final List<HeroicConfig.Builder> configFragments;

    private final Optional<ExecutorService> executor;

    protected HeroicCore(
        Optional<String> id,
        Optional<String> host,
        Optional<Integer> port,
        Optional<Supplier<InputStream>> configStream,
        Optional<Path> configPath,
        Optional<URI> startupPing,
        Optional<String> startupId,
        ExtraParameters params,
        boolean setupService,
        boolean oneshot,
        boolean disableBackends,
        boolean setupShellServer,
        List<HeroicModule> modules,
        List<HeroicProfile> profiles,
        List<HeroicBootstrap> early,
        List<HeroicBootstrap> late,
        List<HeroicConfig.Builder> configFragments,
        Optional<ExecutorService> executor
    ) {
        this.id = id;
        this.host = host;
        this.port = port;
        this.configStream = configStream;
        this.configPath = configPath;
        this.startupPing = startupPing;
        this.startupId = startupId;
        this.params = params;
        this.setupService = setupService;
        this.oneshot = oneshot;
        this.disableBackends = disableBackends;
        this.setupShellServer = setupShellServer;
        this.modules = modules;
        this.profiles = profiles;
        this.early = early;
        this.late = late;
        this.configFragments = configFragments;
        this.executor = executor;
    }

    @Override
    public boolean isDisableLocal() {
        return disableBackends;
    }

    @Override
    public boolean isOneshot() {
        return oneshot;
    }

    /**
     * Start the Heroic core, step by step
     * <p>
     * <p>
     * It sets up the early injector which is responsible for loading all the necessary components
     * to parse a configuration file.
     * <p>
     * <p>
     * Load all the external modules, which are configured in {@link #modules}.
     * <p>
     * <p>
     * Load and build the configuration using the early injector
     * <p>
     * <p>
     * Setup the primary injector which will provide the dependencies to the entire application
     * <p>
     * <p>
     * Run all bootstraps that are configured in {@link #late}
     * <p>
     * <p>
     * Start all the external modules. {@link #startLifeCycles}
     */
    public HeroicCoreInstance newInstance() throws Exception {
        final CoreLoadingComponent loading = loadingInjector();

        loadModules(loading);

        final HeroicConfig config = config(loading);

        String sentryDsn = config.sentryDsn();
        if (sentryDsn != null) {
            Sentry.init(sentryDsn);
        }

        enableTracing(config.tracing());

        final CoreEarlyComponent early = earlyInjector(loading, config);
        runBootstrappers(early, this.early);

        // Initialize the instance injector with access to early components.
        final AtomicReference<CoreComponent> injector = new AtomicReference<>();

        final HeroicCoreInstance instance =
            new Instance(loading.async(), injector, early, config, this.late);

        final CoreComponent primary = primaryInjector(early, config, instance);

        primary.loadingLifeCycle().install();

        primary.internalLifeCycleRegistry().scoped("startup future").start(() -> {
            primary.context().resolveStartedFuture();
            primary.usageTracking().reportStartup();
            return primary.async().resolved(null);
        });

        // Update the instance injector, giving dynamic components initialized after this point
        // access to the primary
        // injector.
        injector.set(primary);

        return instance;
    }

    private void enableTracing(TracingConfig config) throws IOException {
        final TracingConfig.Lightstep lightstep = config.getLightstep();

        if (!lightstep.getCollectorHost().isBlank()) {
            final Options.OptionsBuilder optionsBuilder = new Options.OptionsBuilder()
                .withAccessToken(lightstep.getAccessToken())
                .withMaxReportingIntervalMillis(lightstep.getReportingIntervalMs())
                .withMaxBufferedSpans(lightstep.getMaxBufferedSpans())
                .withCollectorProtocol("http")
                .withResetClient(lightstep.getResetClient())
                .withComponentName(lightstep.getComponentName())
                .withCollectorHost(lightstep.getCollectorHost())
                .withCollectorPort(lightstep.getCollectorPort());

            final Options options;
            try {
                options = optionsBuilder.build();
            } catch (MalformedURLException e) {
                throw new IllegalArgumentException("Malformed configuration for LightStep.", e);
            }

            log.info("Creating LightStep tracing exporter");
            LightStepExporterHandler handler =
                new LightStepExporterHandler(new JRETracer(options));
            log.info("Registering LightStep exporter as the SquashingTraceExporter delegate");
            SquashingTraceExporter.createAndRegister(
                handler,
                config.getSquash().getThreshold(),
                config.getSquash().getWhitelist()
            );

            log.info("Setting global trace probability to {}", config.getProbability());
            TraceConfig traceConfig = Tracing.getTraceConfig();
            traceConfig.updateActiveTraceParams(
                traceConfig
                    .getActiveTraceParams()
                    .toBuilder()
                    .setSampler(Samplers.probabilitySampler(config.getProbability()))
                    .build());
        }

        if (config.getZpagesPort() > 0) {
            log.info("Starting zpage handlers on port {}", config.getZpagesPort());
            // Start a web server for tracing data
            ZPageHandlers.startHttpServerAndRegisterAll(config.getZpagesPort());
        }
    }

    /**
     * This method basically goes through the list of bootstrappers registered by modules and runs
     * them.
     *
     * @param early Injector to inject boostrappers using.
     * @param bootstrappers Bootstraps to run.
     */
    static void runBootstrappers(
        final CoreEarlyComponent early, final List<HeroicBootstrap> bootstrappers
    ) throws Exception {
        for (final HeroicBootstrap bootstrap : bootstrappers) {
            try {
                bootstrap.run(early);
            } catch (Exception e) {
                throw new Exception("Failed to run bootstrapper " + bootstrap, e);
            }
        }
    }

    /**
     * Setup early injector, which is responsible for sufficiently providing dependencies to runtime
     * components.
     */
    private CoreLoadingComponent loadingInjector() {
        log.info("Building Loading Injector");

        final ExecutorService executor;
        final boolean managedExecutor;

        if (this.executor.isPresent()) {
            executor = this.executor.get();
            managedExecutor = false;
        } else {
            executor = setupExecutor(Runtime.getRuntime().availableProcessors() * 2);
            managedExecutor = true;
        }

        return DaggerCoreLoadingComponent
            .builder()
            .loadingModule(new LoadingModule(executor, managedExecutor, this, params))
            .build();
    }

    private CoreEarlyComponent earlyInjector(
        final CoreLoadingComponent loading, final HeroicConfig config
    ) {
        final Optional<String> id = Optionals.firstPresent(this.id, config.id());

        return DaggerCoreEarlyComponent
            .builder()
            .coreLoadingComponent(loading)
            .earlyModule(new EarlyModule(config, id))
            .build();
    }

    /**
     * Setup a fixed thread pool executor that correctly handles unhandled exceptions.
     *
     * @param threads Number of threads to configure.
     */
    private ExecutorService setupExecutor(final int threads) {
        return new ThreadPoolExecutor(threads, threads, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(), new ThreadFactoryBuilder()
            .setNameFormat("heroic-core-%d")
            .setUncaughtExceptionHandler(uncaughtExceptionHandler)
            .build()) {
            @Override
            protected void afterExecute(Runnable r, Throwable t) {
                super.afterExecute(r, t);

                if (t == null && (r instanceof Future<?>)) {
                    try {
                        ((Future<?>) r).get();
                    } catch (CancellationException e) {
                        t = e;
                    } catch (ExecutionException e) {
                        t = e.getCause();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }

                if (t != null) {
                    if (log.isErrorEnabled()) {
                        log.error("Unhandled exception caught in core executor", t);
                        log.error("Exiting (code=2)");
                    } else {
                        System.err.println("Unhandled exception caught in core executor");
                        System.err.println("Exiting (code=2)");
                        t.printStackTrace(System.err);
                    }

                    System.exit(2);
                }
            }
        };
    }

    /**
     * Setup primary injector, which will provide dependencies to the entire application.
     *
     * @param config The loaded configuration file.
     * @param early The early injector, which will act as a parent to the primary injector to bridge
     * all it's provided components.
     * @return The primary guice injector.
     */
    private CoreComponent primaryInjector(
        final CoreEarlyComponent early, final HeroicConfig config, final HeroicCoreInstance instance
    ) {
        log.info("Building Primary Injector");
        final List<LifeCycle> life = new ArrayList<>();

        final StatisticsComponent statistics = config.statistics().module(early);
        life.add(statistics.life());

        final HeroicReporter reporter = statistics.reporter();

        // Register root components.
        final CorePrimaryComponent primary = DaggerCorePrimaryComponent
            .builder()
            .coreEarlyComponent(early)
            .primaryModule(new PrimaryModule(
                instance,
                config.features(),
                reporter,
                config.conditionalFeature(),
                config.tracing()))
            .build();

        final QueryLoggingComponent queryLogging = config.queryLogging().component(primary);

        final UsageTrackingComponent usageTracking = config.usageTracking().module(primary);

        final Optional<HttpServer> server = setupServer(config, life, primary);

        final CacheComponent cache = config.cache().module(primary);
        life.add(cache.cacheLife());

        final AnalyticsComponent analytics = config.analytics().module(primary);
        life.add(analytics.analyticsLife());

        final CoreMetadataComponent metadata = DaggerCoreMetadataComponent
            .builder()
            .primaryComponent(primary)
            .metadataManagerModule(config.metadata())
            .build();

        life.add(metadata.metadataLife());

        final CoreSuggestComponent suggest = DaggerCoreSuggestComponent
            .builder()
            .primaryComponent(primary)
            .suggestManagerModule(config.suggest())
            .build();

        life.add(suggest.suggestLife());

        final CoreMetricComponent metric = DaggerCoreMetricComponent
            .builder()
            .corePrimaryComponent(primary)
            .metadataComponent(metadata)
            .analyticsComponent(analytics)
            .metricManagerModule(config.metric())
            .queryLoggingComponent(queryLogging)
            .build();

        life.add(metric.metricLife());

        final IngestionComponent ingestion =
            config.ingestion().module(primary, suggest, metadata, metric);
        life.add(ingestion.ingestionLife());

        buildShellServer(config).ifPresent(shellServerModule -> {
            final ShellServerComponent shellServer = DaggerShellServerComponent
                .builder()
                .primaryComponent(primary)
                .shellServerModule(shellServerModule)
                .build();

            life.add(shellServer.shellServerLife());
        });

        final CoreClusterComponent cluster = DaggerCoreClusterComponent
            .builder()
            .primaryComponent(primary)
            .metricComponent(metric)
            .metadataComponent(metadata)
            .suggestComponent(suggest)
            .clusterManagerModule(config.cluster())
            .clusterDiscoveryComponent(config.cluster().getDiscovery().module(primary))
            .usageTrackingComponent(usageTracking)
            .build();

        life.add(cluster.clusterLife());

        if (startupPing.isPresent() && startupId.isPresent()) {
            final StartupPingerComponent pinger = DaggerStartupPingerComponent
                .builder()
                .corePrimaryComponent(primary)
                .clusterComponent(cluster)
                .startupPingerModule(
                    new StartupPingerModule(startupPing.get(), startupId.get(), server))
                .build();

            life.add(pinger.startupPingerLife());
        }

        final ConsumersComponent consumer = DaggerCoreConsumersComponent
            .builder()
            .coreConsumersModule(
                new CoreConsumersModule(reporter, config.consumers(), primary, ingestion))
            .corePrimaryComponent(primary)
            .build();
        life.add(consumer.consumersLife());

        final QueryComponent query = DaggerCoreQueryComponent
            .builder()
            .queryModule(new QueryModule(config.metric().groupLimit(),
                config.metric().smallQueryThreshold()))
            .corePrimaryComponent(primary)
            .clusterComponent(cluster)
            .cacheComponent(cache)
            .queryLoggingComponent(queryLogging)
            .build();
        life.add(query.queryLife());

        final GeneratorComponent generator = config.generator().module(primary);
        life.add(generator.generatorLife());

        // install all lifecycles
        final LifeCycle combined = LifeCycle.combined(life);

        combined.install();

        return DaggerCoreComponent
            .builder()
            .primaryComponent(primary)
            .analyticsComponent(analytics)
            .consumersComponent(consumer)
            .metricComponent(metric)
            .metadataComponent(metadata)
            .suggestComponent(suggest)
            .queryComponent(query)
            .queryLoggingComponent(queryLogging)
            .ingestionComponent(ingestion)
            .clusterComponent(cluster)
            .generatorComponent(generator)
            .usageTrackingComponent(usageTracking)
            .build();
    }

    private Optional<HttpServer> setupServer(
        final HeroicConfig config, final List<LifeCycle> life, final CorePrimaryComponent primary
    ) {
        if (!setupService) {
            return Optional.empty();
        }

        final InetSocketAddress bindAddress = setupBindAddress(config);
        final HttpServerComponent serverComponent = DaggerHttpServerComponent
            .builder()
            .primaryComponent(primary)
            .httpServerModule(new HttpServerModule(
                bindAddress,
                config.enableCors(),
                config.corsAllowOrigin(),
                config.connectors(),
                config.tracing()
            ))
            .build();

        // Trigger life cycle registration
        life.add(serverComponent.life());

        return Optional.of(serverComponent.server());
    }

    private Optional<ShellServerModule> buildShellServer(final HeroicConfig config) {
        final Optional<ShellServerModule> shellServer = config.shellServer();

        // a shell server is configured.
        if (shellServer.isPresent()) {
            return shellServer;
        }

        // must have a shell server
        if (setupShellServer) {
            return of(ShellServerModule.builder().build());
        }

        return empty();
    }

    private InetSocketAddress setupBindAddress(HeroicConfig config) {
        final String host =
            Optionals.pickOptional(config.host(), this.host).orElse(DEFAULT_HOST);
        final int port = Optionals.pickOptional(config.port(), this.port).orElse(DEFAULT_PORT);
        return new InetSocketAddress(host, port);
    }

    static void startLifeCycles(
        CoreLifeCycleRegistry registry, CoreComponent primary, HeroicConfig config
    ) throws Exception {
        if (!awaitLifeCycles("start", primary, config.startTimeout(), registry.starters())) {
            throw new Exception("Failed to start all life cycles");
        }
    }

    static void stopLifeCycles(
        CoreLifeCycleRegistry registry, CoreComponent primary, HeroicConfig config
    ) throws Exception {
        if (!awaitLifeCycles("stop", primary, config.stopTimeout(), registry.stoppers())) {
            log.warn("Failed to stop all life cycles");
        }
    }

    static boolean awaitLifeCycles(
        final String op, final CoreComponent primary, final Duration await,
        final List<LifeCycleNamedHook<AsyncFuture<Void>>> hooks
    ) throws InterruptedException, ExecutionException {
        log.info("[{}] {} lifecycle(s)...", op, hooks.size());

        final AsyncFramework async = primary.async();

        final List<AsyncFuture<Void>> futures = new ArrayList<>();
        final List<Pair<AsyncFuture<Void>, LifeCycleHook<AsyncFuture<Void>>>> pairs =
            new ArrayList<>();

        for (final LifeCycleNamedHook<AsyncFuture<Void>> hook : hooks) {
            log.trace("[{}] {}", op, hook.id());

            final AsyncFuture<Void> future;

            try {
                future = hook.get();
            } catch (Exception e) {
                futures.add(async.failed(e));
                break;
            }

            if (log.isTraceEnabled()) {
                final Stopwatch w = Stopwatch.createStarted();

                future.onFinished(() -> {
                    log.trace("[{}] {}, took {}us", op, hook.id(),
                        w.elapsed(TimeUnit.MICROSECONDS));
                });
            }

            futures.add(future);
            pairs.add(Pair.of(future, hook));
        }

        try {
            async.collect(futures).get(await.getDuration(), await.getUnit());
        } catch (final TimeoutException e) {
            log.error("Operation timed out");

            for (final Pair<AsyncFuture<Void>, LifeCycleHook<AsyncFuture<Void>>> pair : pairs) {
                if (!pair.getLeft().isDone()) {
                    log.error("{}: did not finish in time: {}", op, pair.getRight());
                }
            }

            return false;
        }

        log.info("[{}] {} lifecycle(s) done", op, hooks.size());
        return true;
    }

    private HeroicConfig config(LoadingComponent loading) throws Exception {
        HeroicConfig.Builder builder = HeroicConfig.builder();

        for (final HeroicProfile profile : profiles) {
            log.info("Loading profile '{}' (params: {})", profile.description(), params);
            final ExtraParameters p = profile.scope().map(params::scope).orElse(params);
            builder = builder.merge(profile.build(p));
        }

        for (final HeroicConfig.Builder fragment : configFragments) {
            builder = builder.merge(fragment);
        }

        final ObjectMapper mapper = loading.configMapper().copy();

        // TODO: figure out where to put this
        mapper.addMixIn(JettyConnectionFactory.Builder.class, TypeNameMixin.class);

        if (configPath.isPresent()) {
            builder = HeroicConfig
                .loadConfig(mapper, configPath.get())
                .map(builder::merge)
                .orElse(builder);
        }

        if (configStream.isPresent()) {
            builder = HeroicConfig
                .loadConfigStream(mapper, configStream.get().get())
                .map(builder::merge)
                .orElse(builder);
        }

        return builder.build();
    }

    /**
     * Load modules from the specified modules configuration file and wire up those components with
     * early injection.
     *
     * @param loading Injector to wire up modules using.
     */
    private void loadModules(final CoreLoadingComponent loading) {
        final List<HeroicModule> modules = new ArrayList<>();

        for (final HeroicModule builtin : BUILTIN_MODULES) {
            modules.add(builtin);
        }

        modules.addAll(this.modules);

        for (final HeroicModule module : modules) {
            // inject members of an entry point and run them.
            module.setup(loading).run();
        }

        log.info("Loaded {} module(s)", modules.size());
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<String> id = empty();
        private Optional<String> host = empty();
        private Optional<Integer> port = empty();
        private Optional<Supplier<InputStream>> configStream = empty();
        private Optional<Path> configPath = empty();
        private Optional<ExtraParameters> params = empty();
        private Optional<HeroicReporter> reporter = empty();
        private Optional<URI> startupPing = empty();
        private Optional<String> startupId = empty();

        /* flags */
        private Optional<Boolean> setupService = empty();
        private Optional<Boolean> oneshot = empty();
        private Optional<Boolean> disableBackends = empty();
        private Optional<Boolean> setupShellServer = empty();

        /* extensions */
        private final ImmutableList.Builder<HeroicModule> modules = ImmutableList.builder();
        private final ImmutableList.Builder<HeroicProfile> profiles = ImmutableList.builder();
        private final ImmutableList.Builder<HeroicBootstrap> early = ImmutableList.builder();
        private final ImmutableList.Builder<HeroicBootstrap> late = ImmutableList.builder();

        /* configuration fragments */
        private final ImmutableList.Builder<HeroicConfig.Builder> configFragments =
            ImmutableList.builder();

        private Optional<ExecutorService> executor = empty();

        public Builder executor(final ExecutorService executor) {
            this.executor = of(executor);
            return this;
        }

        /**
         * Register a specific ID for this heroic instance.
         *
         * @param id Id of the instance.
         * @return This builder.
         */
        public Builder id(final String id) {
            this.id = of(id);
            return this;
        }

        /**
         * If a shell server is not configured, setup the default shell server.
         */
        public Builder setupShellServer(boolean setupShellServer) {
            this.setupShellServer = of(setupShellServer);
            return this;
        }

        /**
         * Port to bind service to.
         */
        public Builder port(final int port) {
            this.port = of(port);
            return this;
        }

        /**
         * Host to bind service to.
         */
        public Builder host(final String host) {
            checkNotNull(host, "host");
            this.host = of(host);
            return this;
        }

        public Builder configStream(final Supplier<InputStream> configStream) {
            checkNotNull(configStream, "configStream");
            this.configStream = of(configStream);
            return this;
        }

        public Builder configPath(final String configPath) {
            checkNotNull(configPath, "configPath");
            return configPath(Paths.get(configPath));
        }

        public Builder configPath(final Path configPath) {
            checkNotNull(configPath, "configPath");

            if (!Files.isReadable(configPath)) {
                throw new IllegalArgumentException(
                    "Configuration is not readable: " + configPath.toAbsolutePath());
            }

            this.configPath = of(configPath);
            return this;
        }

        /**
         * Programmatically add a configuration fragment.
         *
         * @param config Configuration fragment to add.
         * @return This builder.
         */
        public Builder configFragment(final HeroicConfig.Builder config) {
            checkNotNull(config, "config");
            this.configFragments.add(config);
            return this;
        }

        public Builder startupPing(final String startupPing) {
            checkNotNull(startupPing, "startupPing");
            this.startupPing = of(URI.create(startupPing));
            return this;
        }

        public Builder startupId(final String startupId) {
            checkNotNull(startupId, "startupId");
            this.startupId = of(startupId);
            return this;
        }

        public Builder parameters(final ExtraParameters params) {
            checkNotNull(params, "params");
            this.params = of(params);
            return this;
        }

        public Builder reporter(final HeroicReporter reporter) {
            checkNotNull(reporter, "reporter");
            this.reporter = of(reporter);
            return this;
        }

        /**
         * Configure setup of the server component of heroic or not.
         */
        public Builder setupService(final boolean setupService) {
            this.setupService = of(setupService);
            return this;
        }

        /**
         * Do not perform any scheduled tasks, only perform then once during startup.
         */
        public Builder oneshot(final boolean oneshot) {
            this.oneshot = of(oneshot);
            return this;
        }

        /**
         * Disable local backends.
         */
        public Builder disableBackends(final boolean disableBackends) {
            this.disableBackends = of(disableBackends);
            return this;
        }

        public Builder modules(final List<HeroicModule> modules) {
            checkNotNull(modules, "modules");
            this.modules.addAll(modules);
            return this;
        }

        public Builder module(final HeroicModule module) {
            checkNotNull(module, "module");
            this.modules.add(module);
            return this;
        }

        public Builder profiles(final List<HeroicProfile> profiles) {
            checkNotNull(profiles, "profiles");
            this.profiles.addAll(profiles);
            return this;
        }

        public Builder profile(final HeroicProfile profile) {
            checkNotNull(profile, "profile");
            this.profiles.add(profile);
            return this;
        }

        public Builder earlyBootstrap(final HeroicBootstrap bootstrap) {
            checkNotNull(bootstrap, "bootstrap");
            this.early.add(bootstrap);
            return this;
        }

        public Builder bootstrappers(final List<HeroicBootstrap> bootstrappers) {
            checkNotNull(bootstrappers, "bootstrappers");
            this.late.addAll(bootstrappers);
            return this;
        }

        public Builder bootstrap(final HeroicBootstrap bootstrap) {
            checkNotNull(bootstrap, "bootstrap");
            this.late.add(bootstrap);
            return this;
        }

        public HeroicCore build() {
            // @formatter:off
            return new HeroicCore(
                id,
                host,
                port,
                configStream,
                configPath,
                startupPing,
                startupId,

                params.orElseGet(ExtraParameters::empty),

                setupService.orElse(DEFAULT_SETUP_SERVICE),
                oneshot.orElse(DEFAULT_ONESHOT),
                disableBackends.orElse(DEFAULT_DISABLE_BACKENDS),
                setupShellServer.orElse(DEFAULT_SETUP_SHELL_SERVER),

                modules.build(),
                profiles.build(),
                early.build(),
                late.build(),

                configFragments.build(),

                executor
            );
            // @formatter:on
        }
    }

    public static HeroicModule[] builtinModules() {
        return BUILTIN_MODULES;
    }

    public static class Instance implements HeroicCoreInstance {
        private final AtomicReference<CoreComponent> coreInjector;
        private final CoreEarlyComponent early;
        private final HeroicConfig config;
        private final List<HeroicBootstrap> late;

        private final ResolvableFuture<Void> start;
        private final ResolvableFuture<Void> stop;

        private final AsyncFuture<Void> started;
        private final AsyncFuture<Void> stopped;

        public Instance(
            final AsyncFramework async, final AtomicReference<CoreComponent> coreInjector,
            final CoreEarlyComponent early, final HeroicConfig config,
            final List<HeroicBootstrap> late
        ) {
            this.coreInjector = coreInjector;
            this.early = early;
            this.config = config;
            this.late = late;

            this.start = async.future();
            this.stop = async.future();

            // Required because of https://github.com/elastic/elasticsearch/issues/25741
            System.setProperty("es.set.netty.runtime.available.processors", "false");

            this.started = start.directTransform(n -> {
                final CoreComponent core = coreInjector.get();
                assert core != null;

                final CoreLifeCycleRegistry coreRegistry =
                    (CoreLifeCycleRegistry) core.lifeCycleRegistry();

                runBootstrappers(early, late);
                startLifeCycles(coreRegistry, core, config);

                final CoreLifeCycleRegistry internalRegistry =
                    (CoreLifeCycleRegistry) core.internalLifeCycleRegistry();

                startLifeCycles(internalRegistry, core, config);

                log.info("Startup finished, hello!");
                return null;
            });

            this.stopped = async.collect(ImmutableList.of(started, stop)).lazyTransform(n -> {
                return async.call(() -> {
                    final CoreComponent core = coreInjector.get();
                    assert core != null;

                    final CoreLifeCycleRegistry coreRegistry =
                        (CoreLifeCycleRegistry) core.lifeCycleRegistry();

                    core.stopSignal().run();

                    log.info("Shutting down Heroic");

                    try {
                        stopLifeCycles(coreRegistry, core, config);
                    } catch (Exception e) {
                        log.error("Failed to stop all lifecycles, continuing anyway...", e);
                    }

                    log.info("Stopping internal life cycle");

                    final CoreLifeCycleRegistry internalRegistry =
                        (CoreLifeCycleRegistry) core.internalLifeCycleRegistry();

                    try {
                        stopLifeCycles(internalRegistry, core, config);
                    } catch (Exception e) {
                        log.error("Failed to stop all internal lifecycles, continuing anyway...",
                            e);
                    }

                    log.info("Done shutting down, bye bye!");
                    return null;
                }, Executors.newSingleThreadExecutor());
            });
        }

        /**
         * Inject fields to the provided injector using the primary injector.
         *
         * @param injector Injector to use.
         */
        @Override
        public <T> T inject(Function<CoreComponent, T> injector) {
            final CoreComponent c = coreInjector.get();

            if (c == null) {
                throw new IllegalStateException(
                    "Injection attempted before instance has been fully initialize, use " +
                        "LifeCycleRegistry#start(..) to register a hook instead");
            }

            return injector.apply(c);
        }

        @Override
        public AsyncFuture<Void> start() {
            this.start.resolve(null);
            return this.started;
        }

        @Override
        public AsyncFuture<Void> shutdown() {
            this.stop.resolve(null);
            return this.stopped;
        }

        @Override
        public AsyncFuture<Void> join() {
            return this.stopped;
        }
    }
}
