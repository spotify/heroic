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

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.spotify.heroic.HeroicInternalLifeCycle.Context;
import com.spotify.heroic.cluster.LocalClusterNode;
import com.spotify.heroic.common.Duration;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.Optionals;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.scheduler.Scheduler;
import com.spotify.heroic.shell.ShellServerModule;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.noop.NoopHeroicReporter;

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Configure and bootstrap a Heroic application.
 *
 * All public methods are thread-safe.
 *
 * All fields are non-null.
 *
 * @author udoprog
 */
@Slf4j
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class HeroicCore implements HeroicConfiguration, HeroicReporterConfiguration {
    static final String DEFAULT_HOST = "0.0.0.0";
    static final int DEFAULT_PORT = 8080;

    static final boolean DEFAULT_SETUP_SERVICE = true;
    static final boolean DEFAULT_ONESHOT = false;
    static final boolean DEFAULT_DISABLE_BACKENDS = false;
    static final boolean DEFAULT_SKIP_LIFECYCLES = false;
    static final boolean DEFAULT_SETUP_SHELL_SERVER = true;

    static final String APPLICATION_JSON_INTERNAL = "application/json+internal";
    static final String APPLICATION_JSON = "application/json";
    static final String APPLICATION_HEROIC_CONFIG = "application/heroic-config";

    static final UncaughtExceptionHandler uncaughtExceptionHandler =
            new UncaughtExceptionHandler() {
                @Override
                public void uncaughtException(Thread t, Throwable e) {
                    try {
                        System.err.println(String
                                .format("Uncaught exception caught in thread %s, exiting...", t));
                        e.printStackTrace(System.err);
                    } finally {
                        System.exit(1);
                    }
                }
            };

    /**
     * Built-in modules that should always be loaded.
     */
    // @formatter:off
    private static final HeroicModule[] BUILTIN_MODULES = new HeroicModule[] {
        new com.spotify.heroic.aggregation.Module(),
        new com.spotify.heroic.filter.Module(),
        new com.spotify.heroic.http.Module()
    };
    // @formatter:on

    /**
     * Global lock that must be acquired when starting and shutting down core.
     *
     * @see {@link #start()}
     * @see {@link #shutdown()}
     */
    private final Optional<String> host;
    private final Optional<Integer> port;
    private final Optional<Path> configPath;
    private final Optional<URI> startupPing;
    private final Optional<String> startupId;

    /**
     * Additional dynamic parameters to pass into the configuration of a profile. These are
     * typically extracted from the commandline, or a properties file.
     */
    private final ExtraParameters params;

    /**
     * Root entry for the metric reporter.
     */
    private final AtomicReference<HeroicReporter> reporter;

    /* flags */
    private final boolean setupService;
    private final boolean oneshot;
    private final boolean disableBackends;
    private final boolean skipLifecycles;
    private final boolean setupShellServer;

    /* extensions */
    private final List<HeroicModule> modules;
    private final List<HeroicProfile> profiles;
    private final List<HeroicBootstrap> early;
    private final List<HeroicBootstrap> late;

    @Override
    public void registerReporter(final HeroicReporter reporter) {
        this.reporter.set(reporter);
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
     * Main entry point, will block until service is finished.
     *
     * @throws Exception
     */
    public HeroicCoreInstance start() throws Exception {
        return doStart();
    }

    /**
     * Start the Heroic core, step by step
     *
     * <p>
     * It sets up the early injector which is responsible for loading all the necessary components
     * to parse a configuration file.
     *
     * <p>
     * Load all the external modules, which are configured in {@link #modules}.
     *
     * <p>
     * Load and build the configuration using the early injector
     *
     * <p>
     * Setup the primary injector which will provide the dependencies to the entire application
     *
     * <p>
     * Run all bootstraps that are configured in {@link #late}
     *
     * <p>
     * Start all the external modules. {@link #startLifeCycles}
     *
     * <p>
     * Start all the internal modules. {@link #startInternalLifecycles}
     *
     * @throws Exception
     */
    private HeroicCoreInstance doStart() throws Exception {
        final Injector loading = loadingInjector();

        loadModules(loading);

        final HeroicConfig config = config(loading);

        final Injector early = earlyInjector(loading, config);
        runBootstrappers(early, this.early);

        // Initialize the instance injector with access to early components.
        final AtomicReference<Injector> injector = new AtomicReference<>(early);

        final HeroicCoreInstance instance = setupInstance(injector);

        final Injector primary = primaryInjector(early, config, instance);

        // Update the instance injector, giving dynamic components initialized after this point
        // access to the primary
        // injector.
        injector.set(primary);

        // Must happen after updating the instance injector above.
        fetchAllBindings(primary);

        runBootstrappers(primary, this.late);
        startLifeCycles(primary);
        startInternalLifecycles(primary);

        log.info("Heroic was successfully started!");
        return instance;
    }

    /**
     * Goes through _all_ bindings for the given injector and gets their value to make sure they
     * have been initialized.
     */
    private void fetchAllBindings(final Injector injector) {
        for (final Entry<Key<?>, Binding<?>> entry : injector.getAllBindings().entrySet()) {
            entry.getValue().getProvider().get();
        }
    }

    private HeroicCoreInstance setupInstance(final AtomicReference<Injector> coreInjector) {
        return new HeroicCoreInstance() {
            private final Object lock = new Object();

            private volatile boolean stopped = false;

            /**
             * Inject fields to the provided injectee using the primary injector.
             *
             * @param injectee Object to inject fields on.
             */
            @Override
            public <T> T inject(T injectee) {
                coreInjector.get().injectMembers(injectee);
                return injectee;
            }

            @Override
            public <T> T injectInstance(Class<T> cls) {
                return coreInjector.get().getInstance(cls);
            }

            @SuppressFBWarnings("DM_GC")
            @Override
            public void shutdown() {
                final Injector injector = coreInjector.get();

                synchronized (lock) {
                    if (stopped) {
                        return;
                    }

                    final HeroicInternalLifeCycle lifecycle =
                            injector.getInstance(HeroicInternalLifeCycle.class);

                    log.info("Shutting down Heroic");

                    try {
                        stopLifeCycles(injector);
                    } catch (Exception e) {
                        log.error("Failed to stop all lifecycles, continuing anyway...", e);
                    }

                    log.info("Stopping internal life cycle");
                    lifecycle.stop();

                    /**
                     * perform a gc to try to cause any dangling references to be logged through
                     * their {@link Object#finalize()} method.
                     */
                    Runtime.getRuntime().gc();

                    log.info("Done shutting down, bye bye!");
                    stopped = true;
                }
            }

            @Override
            public void join() throws InterruptedException {
                synchronized (lock) {
                    while (!stopped) {
                        lock.wait();
                    }
                }
            }
        };
    }

    /**
     * Start the internal lifecycles
     *
     * First step is to register event hooks that makes sure that the lifecycle components gets
     * started and shutdown correctly. After this the registered internal lifecycles are started.
     *
     * @param primary
     */
    private void startInternalLifecycles(final Injector primary) {
        final HeroicInternalLifeCycle lifecycle =
                primary.getInstance(HeroicInternalLifeCycle.class);

        lifecycle.registerShutdown("Core Scheduler", new HeroicInternalLifeCycle.ShutdownHook() {
            @Override
            public void onShutdown() throws Exception {
                primary.getInstance(Scheduler.class).stop();
            }
        });

        lifecycle.registerShutdown("Core Executor Service",
                new HeroicInternalLifeCycle.ShutdownHook() {
                    @Override
                    public void onShutdown() throws Exception {
                        primary.getInstance(ExecutorService.class).shutdown();
                    }
                });

        lifecycle.register("Core Future Resolver", new HeroicInternalLifeCycle.StartupHook() {
            @Override
            public void onStartup(Context context) throws Exception {
                final CoreHeroicContext heroicContext =
                        (CoreHeroicContext) primary.getInstance(HeroicContext.class);
                heroicContext.resolveCoreFuture();
            }
        });

        lifecycle.start();
    }

    /**
     * This method basically goes through the list of bootstrappers registered by modules and runs
     * them.
     *
     * @param injector Injector to inject boostrappers using.
     * @param bootstrappers Bootstrappers to run.
     * @throws Exception
     */
    private void runBootstrappers(final Injector injector,
            final List<HeroicBootstrap> bootstrappers) throws Exception {
        for (final HeroicBootstrap bootstrap : bootstrappers) {
            try {
                injector.injectMembers(bootstrap);
                bootstrap.run();
            } catch (Exception e) {
                throw new Exception("Failed to run bootstrapper " + bootstrap, e);
            }
        }
    }

    /**
     * Setup early injector, which is responsible for sufficiently providing dependencies to runtime
     * components.
     */
    private Injector loadingInjector() {
        log.info("Building Loading Injector");

        final ExecutorService executor =
                buildCoreExecutor(Runtime.getRuntime().availableProcessors() * 2);
        final HeroicInternalLifeCycle lifeCycle = new HeroicInternalLifeCycleImpl();

        return Guice
                .createInjector(new HeroicLoadingModule(executor, lifeCycle, this, this, params));
    }

    private Injector earlyInjector(final Injector loading, final HeroicConfig config) {
        return loading.createChildInjector(new HeroicEarlyModule(config));
    }

    /**
     * Setup a fixed thread pool executor that correctly handles unhandled exceptions.
     *
     * @param nThreads Number of threads to configure.
     * @return
     */
    private ExecutorService buildCoreExecutor(final int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryBuilder().setNameFormat("heroic-core-%d")
                        .setUncaughtExceptionHandler(uncaughtExceptionHandler).build()) {
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
                    log.error("Unhandled exception caught in core executor", t);
                    log.error("Exiting (code=2)");
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
     *            all it's provided components.
     * @return The primary guice injector.
     */
    private Injector primaryInjector(final Injector early, final HeroicConfig config,
            final HeroicCoreInstance instance) {
        log.info("Building Primary Injector");

        final List<Module> modules = new ArrayList<Module>();

        final Set<LifeCycle> lifeCycles = new HashSet<>();

        final InetSocketAddress bindAddress = setupBindAddress(config);

        final Optional<HeroicStartupPinger> pinger;

        if (startupPing.isPresent() && startupId.isPresent()) {
            pinger = of(new HeroicStartupPinger(startupPing.get(), startupId.get()));
        } else {
            pinger = empty();
        }

        final HeroicReporter reporter = this.reporter.get();

        // register root components.
        modules.add(
                new HeroicPrimaryModule(instance, lifeCycles, bindAddress, config.isEnableCors(),
                        config.getCorsAllowOrigin(), setupService, reporter, pinger));

        if (!disableBackends) {
            modules.add(new AbstractModule() {
                @Override
                protected void configure() {
                    bind(LocalClusterNode.class).in(Scopes.SINGLETON);
                }
            });

            modules.add(config.getMetric());
            modules.add(config.getMetadata());
            modules.add(config.getSuggest());
            modules.add(config.getIngestion());
        }

        Optional<ShellServerModule> shellServer = buildShellServer(config);

        if (shellServer.isPresent()) {
            modules.add(shellServer.get());
        }

        modules.add(config.getCluster().make(this));
        modules.add(config.getCache());

        modules.add(new AbstractModule() {
            @Override
            protected void configure() {
                for (final Module m : setupConsumers(config, reporter, binder())) {
                    install(m);
                }
            }
        });

        // make new injector child of early injector so they can access everything in it.
        return early.createChildInjector(modules);
    }

    private Optional<ShellServerModule> buildShellServer(final HeroicConfig config) {
        final Optional<ShellServerModule> shellServer = config.getShellServer();

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
                Optionals.pickOptional(config.getHost(), this.host).orElse(DEFAULT_HOST);
        final int port = Optionals.pickOptional(config.getPort(), this.port).orElse(DEFAULT_PORT);
        return new InetSocketAddress(host, port);
    }

    private static List<Module> setupConsumers(final HeroicConfig config,
            final HeroicReporter reporter, Binder binder) {
        final Multibinder<Consumer> bindings = Multibinder.newSetBinder(binder, Consumer.class);

        final List<Module> modules = new ArrayList<>();

        final AtomicInteger index = new AtomicInteger();

        for (final ConsumerModule consumer : config.getConsumers()) {
            final String id =
                    consumer.id().orElseGet(() -> consumer.buildId(index.getAndIncrement()));
            final Key<Consumer> key = Key.get(Consumer.class, Names.named(id));
            modules.add(consumer.module(key, reporter.newConsumer(id)));
            bindings.addBinding().to(key);
        }

        return modules;
    }

    private void startLifeCycles(Injector primary) throws Exception {
        log.info("Starting life cycles");

        final HeroicConfig config = primary.getInstance(HeroicConfig.class);

        if (!awaitLifeCycles("start", primary, config.getStartTimeout(), LifeCycle::start)) {
            throw new Exception("Failed to start all life cycles");
        }

        log.info("Started all life cycles");
    }

    private void stopLifeCycles(final Injector primary) throws Exception {
        log.info("Stopping life cycles");

        final HeroicConfig config = primary.getInstance(HeroicConfig.class);

        if (!awaitLifeCycles("stop", primary, config.getStopTimeout(), LifeCycle::stop)) {
            log.warn("Failed to stop all life cycles");
            return;
        }

        log.info("Stopped all life cycles");
    }

    boolean awaitLifeCycles(final String op, final Injector primary, final Duration await,
            final Function<LifeCycle, AsyncFuture<Void>> fn)
                    throws InterruptedException, ExecutionException {
        final AsyncFramework async = primary.getInstance(AsyncFramework.class);

        // we still need to get instances to cause them to be created.
        final Set<LifeCycle> lifeCycles =
                primary.getInstance(Key.get(new TypeLiteral<Set<LifeCycle>>() {
                }));

        if (skipLifecycles) {
            log.info("{}: skipping (skipLifecycles = true)", op);
            return true;
        }

        final List<AsyncFuture<Void>> futures = new ArrayList<>();
        final List<Pair<AsyncFuture<Void>, LifeCycle>> pairs = new ArrayList<>();

        for (final LifeCycle l : lifeCycles) {
            log.info("{}: running {}", op, l);

            final AsyncFuture<Void> future = fn.apply(l).onDone(new FutureDone<Void>() {
                @Override
                public void failed(Throwable cause) throws Exception {
                    log.info("{}: failed: {}", op, l, cause);
                }

                @Override
                public void resolved(Void result) throws Exception {
                    log.info("{}: done: {}", op, l);
                }

                @Override
                public void cancelled() throws Exception {
                    log.info("{}: cancelled: {}", op, l);
                }
            });

            futures.add(future);
            pairs.add(Pair.of(future, l));
        }

        try {
            async.collect(futures).get(await.getDuration(), await.getUnit());
        } catch (TimeoutException e) {
            log.error("Operation timed out");

            for (final Pair<AsyncFuture<Void>, LifeCycle> pair : pairs) {
                if (!pair.getLeft().isDone()) {
                    log.error("{}: did not finish in time: {}", op, pair.getRight());
                }
            }

            return false;
        }

        return true;
    }

    private HeroicConfig config(Injector earlyInjector) throws Exception {
        HeroicConfig.Builder builder = HeroicConfig.builder();

        for (final HeroicProfile profile : profiles) {
            log.info("Loading profile '{}' (params: {})", profile.description(), params);
            builder = builder.merge(profile.build(params));
        }

        final HeroicConfig.Builder b = builder;
        return configPath.map(c -> b.merge(loadConfig(earlyInjector, c))).orElse(b).build();
    }

    /**
     * Load modules from the specified modules configuration file and wire up those components with
     * early injection.
     *
     * @param injector Injector to wire up modules using.
     * @throws IOException
     */
    private void loadModules(final Injector injector) throws Exception {
        final List<HeroicModule> modules = new ArrayList<>();

        for (final HeroicModule builtin : BUILTIN_MODULES) {
            modules.add(builtin);
        }

        modules.addAll(this.modules);

        for (final HeroicModule module : modules) {
            log.info("Loading Module: {}", module.getClass().getPackage().getName());
            // inject members of an entry point and run them.
            final HeroicModule.Entry entry = module.setup();
            injector.injectMembers(entry);
            entry.setup();
        }
    }

    private HeroicConfig.Builder loadConfig(final Injector earlyInjector, final Path path) {
        final ObjectMapper mapper = earlyInjector
                .getInstance(Key.get(ObjectMapper.class, Names.named(APPLICATION_HEROIC_CONFIG)));

        try {
            return mapper.readValue(Files.newInputStream(path), HeroicConfig.Builder.class);
        } catch (final JsonMappingException e) {
            final JsonLocation location = e.getLocation();
            final String message = String.format("%s[%d:%d]: %s", configPath,
                    location == null ? null : location.getLineNr(),
                    location == null ? null : location.getColumnNr(), e.getOriginalMessage());
            throw new RuntimeException(message, e);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private Optional<String> host = empty();
        private Optional<Integer> port = empty();
        private Optional<Path> configPath = empty();
        private Optional<ExtraParameters> params = empty();
        private Optional<HeroicReporter> reporter = empty();
        private Optional<URI> startupPing = empty();
        private Optional<String> startupId = empty();

        /* flags */
        private Optional<Boolean> setupService = empty();
        private Optional<Boolean> oneshot = empty();
        private Optional<Boolean> disableBackends = empty();
        private Optional<Boolean> skipLifecycles = empty();
        private Optional<Boolean> setupShellServer = empty();

        /* extensions */
        private final ImmutableList.Builder<HeroicModule> modules = ImmutableList.builder();
        private final ImmutableList.Builder<HeroicProfile> profiles = ImmutableList.builder();
        private final ImmutableList.Builder<HeroicBootstrap> early = ImmutableList.builder();
        private final ImmutableList.Builder<HeroicBootstrap> late = ImmutableList.builder();

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

        /**
         * Skip startup of lifecycles.
         */
        public Builder skipLifecycles(final boolean skipLifecycles) {
            this.skipLifecycles = of(skipLifecycles);
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
                host,
                port,
                configPath,
                startupPing,
                startupId,

                params.orElseGet(ExtraParameters::empty),
                new AtomicReference<>(reporter.orElseGet(NoopHeroicReporter::get)),

                setupService.orElse(DEFAULT_SETUP_SERVICE),
                oneshot.orElse(DEFAULT_ONESHOT),
                disableBackends.orElse(DEFAULT_DISABLE_BACKENDS),
                skipLifecycles.orElse(DEFAULT_SKIP_LIFECYCLES),
                setupShellServer.orElse(DEFAULT_SETUP_SHELL_SERVER),

                modules.build(),
                profiles.build(),
                early.build(),
                late.build()
            );
            // @formatter:on
        }
    }

    public static HeroicModule[] builtinModules() {
        return BUILTIN_MODULES;
    }
}
