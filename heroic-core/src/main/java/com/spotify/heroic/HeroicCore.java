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

import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
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
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.scheduler.Scheduler;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.noop.NoopHeroicReporter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import lombok.extern.slf4j.Slf4j;

/**
 * Configure and bootstrap a Heroic application.
 *
 * All public methods are thread-safe.
 *
 * @author udoprog
 */
@Slf4j
public class HeroicCore implements HeroicCoreInjector, HeroicOptions {
    static final List<Class<?>> DEFAULT_MODULES = ImmutableList.of();
    static final boolean DEFAULT_SERVER = true;
    static final String DEFAULT_HOST = "0.0.0.0";
    static final int DEFAULT_PORT = 8080;
    static final boolean DEFAULT_ONESHOT = false;

    /**
     * Which resource file to load modules from.
     */
    static final String APPLICATION_JSON_INTERNAL = "application/json+internal";
    static final String APPLICATION_JSON = "application/json";
    static final String APPLICATION_HEROIC_CONFIG = "application/heroic-config";

    static final UncaughtExceptionHandler uncaughtExceptionHandler = new UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            try {
                System.err.println(String.format("Uncaught exception caught in thread %s, exiting...", t));
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
    private static final Class<?>[] BUILTIN_MODULES = new Class<?>[] {
        com.spotify.heroic.aggregation.Entry.class,
        com.spotify.heroic.filter.Entry.class,
        com.spotify.heroic.http.Entry.class
    };
    // @formatter:on

    private final AtomicReference<Injector> primary = new AtomicReference<>();

    private final String host;
    private final Integer port;
    private final List<Class<?>> modules;
    private final List<HeroicBootstrap> bootstrappers;
    private final boolean server;
    private final Path configPath;
    private final HeroicProfile profile;
    private final HeroicReporter reporter;
    private final URI startupPing;
    private final String startupId;
    private final boolean oneshot;
    private final boolean disableBackends;
    private final boolean skipLifecycles;

    public HeroicCore(String host, Integer port, List<Class<?>> modules, List<HeroicBootstrap> bootstrappers,
            Boolean server, Path configPath, HeroicProfile profile, HeroicReporter reporter, URI startupPing,
            String startupId, boolean oneshot, boolean disableBackends, boolean skipLifecycles) {
        this.host = Optional.fromNullable(host).or(DEFAULT_HOST);
        this.port = port;
        this.modules = Optional.fromNullable(modules).or(DEFAULT_MODULES);
        this.bootstrappers = ImmutableList.copyOf(Optional.fromNullable(bootstrappers).or(ImmutableList.of()));
        this.server = Optional.fromNullable(server).or(DEFAULT_SERVER);
        this.configPath = configPath;
        this.profile = profile;
        this.reporter = Optional.fromNullable(reporter).or(NoopHeroicReporter.get());
        this.startupPing = startupPing;
        this.startupId = startupId;
        this.oneshot = oneshot;
        this.disableBackends = disableBackends;
        this.skipLifecycles = skipLifecycles;
    }

    @Override
    public boolean isDisableLocal() {
        return disableBackends;
    }

    @Override
    public boolean isOneshot() {
        return oneshot;
    }

    private final Object $lock = new Object();

    /**
     * Main entry point, will block until service is finished.
     *
     * @throws Exception
     */
    public void start() throws Exception {
        if (primary.get() != null) {
            return;
        }

        synchronized ($lock) {
            if (primary.get() != null) {
                return;
            }

            doStart();
        }
    }

    private void doStart() throws Exception {
        final Injector early = earlyInjector();

        loadModules(early);

        final HeroicConfig config = config(early);
        final Injector primary = primaryInjector(config, early);

        for (final HeroicBootstrap bootstrap : bootstrappers) {
            try {
                primary.injectMembers(bootstrap);
                bootstrap.run();
            } catch (Exception e) {
                throw new Exception("Failed to run bootstrapper " + bootstrap, e);
            }
        }

        this.primary.set(primary);

        try {
            startLifeCycles(primary);
        } catch (Exception e) {
            throw new Exception("Failed to start all lifecycles", e);
        }

        final HeroicInternalLifeCycle lifecycle = primary.getInstance(HeroicInternalLifeCycle.class);

        lifecycle.registerShutdown("Core Scheduler", new HeroicInternalLifeCycle.ShutdownHook() {
            @Override
            public void onShutdown() throws Exception {
                primary.getInstance(Scheduler.class).stop();
            }
        });

        lifecycle.registerShutdown("Core Executor Service", new HeroicInternalLifeCycle.ShutdownHook() {
            @Override
            public void onShutdown() throws Exception {
                primary.getInstance(ExecutorService.class).shutdown();
            }
        });

        lifecycle.register("Core Future Resolver", new HeroicInternalLifeCycle.StartupHook() {
            @Override
            public void onStartup(Context context) throws Exception {
                final CoreHeroicContext heroicContext = (CoreHeroicContext) primary.getInstance(HeroicContext.class);
                heroicContext.resolveCoreFuture();
            }
        });

        lifecycle.start();
        log.info("Heroic was successfully started!");
    }

    /**
     * Inject fields to the provided injectee using the primary injector.
     *
     * @param injectee Object to inject fields on.
     */
    @Override
    public <T> T inject(T injectee) {
        final Injector primary = this.primary.get();

        if (primary == null) {
            throw new IllegalStateException("primary injector not available");
        }

        primary.injectMembers(injectee);
        return injectee;
    }

    @Override
    public <T> T injectInstance(Class<T> cls) {
        final Injector primary = this.primary.get();

        if (primary == null) {
            throw new IllegalStateException("primary injector not available");
        }

        return primary.getInstance(cls);
    }

    public void join() throws InterruptedException {
        synchronized ($lock) {
            while (primary.get() != null) {
                $lock.wait();
            }
        }
    }

    public void shutdown() {
        synchronized ($lock) {
            final Injector primary = this.primary.getAndSet(null);

            if (primary == null) {
                return;
            }

            final HeroicInternalLifeCycle lifecycle = primary.getInstance(HeroicInternalLifeCycle.class);

            log.info("Shutting down Heroic");

            try {
                stopLifeCycles(primary);
            } catch (Exception e) {
                log.error("Failed to stop all lifecycles, continuing anyway...", e);
            }

            log.info("Stopping internal life cycle");
            lifecycle.stop();

            // perform a gc to try to cause any dangling references to be logged through their {@code Object#finalize()}
            // method.
            Runtime.getRuntime().gc();

            log.info("Done shutting down, bye bye!");
            $lock.notifyAll();
        }
    }

    /**
     * Setup early injector, which is responsible for sufficiently providing dependencies to runtime components.
     */
    private Injector earlyInjector() {
        log.info("Building Early Injector");

        final ExecutorService executor = buildCoreExecutor(Runtime.getRuntime().availableProcessors() * 2);
        final HeroicInternalLifeCycle lifeCycle = new HeroicInernalLifeCycleImpl();

        return Guice.createInjector(new HeroicEarlyModule(executor, lifeCycle, this));
    }

    /**
     * Setup a fixed thread pool executor that correctly handles unhandled exceptions.
     *
     * @param nThreads Number of threads to configure.
     * @return
     */
    private ExecutorService buildCoreExecutor(final int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads,
                 0L, TimeUnit.MILLISECONDS,
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
     * @param early The early injector, which will act as a parent to the primary injector to bridge all it's provided
     *            components.
     * @return The primary guice injector.
     */
    private Injector primaryInjector(final HeroicConfig config, final Injector early) {
        log.info("Building Primary Injector");

        final List<Module> modules = new ArrayList<Module>();

        final Set<LifeCycle> lifeCycles = new HashSet<>();

        final InetSocketAddress bindAddress = setupBindAddress(config);

        final HeroicStartupPinger pinger;

        if (startupPing != null && startupId != null) {
            pinger = new HeroicStartupPinger(startupPing, startupId);
        } else {
            pinger = null;
        }

        // register root components.
        modules.add(new HeroicPrimaryModule(this, lifeCycles, config, bindAddress, server, reporter, pinger));

        modules.add(config.getClient());

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

            config.getShellServer().transform(modules::add);
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
        final Injector injector = early.createChildInjector(modules);

        // touch all bindings to make sure they are 'eagerly' initialized.
        for (final Entry<Key<?>, Binding<?>> entry : injector.getAllBindings().entrySet()) {
            entry.getValue().getProvider().get();
        }

        return injector;
    }

    private InetSocketAddress setupBindAddress(HeroicConfig config) {
        final String host = Optional.fromNullable(this.host).or(
                Optional.fromNullable(config.getHost()).or(DEFAULT_HOST));
        final int port = Optional.fromNullable(this.port).or(Optional.fromNullable(config.getPort()).or(DEFAULT_PORT));
        return new InetSocketAddress(host, port);
    }

    private static List<Module> setupConsumers(final HeroicConfig config, final HeroicReporter reporter, Binder binder) {
        final Multibinder<Consumer> bindings = Multibinder.newSetBinder(binder, Consumer.class);

        final List<Module> modules = new ArrayList<>();

        int index = 0;

        for (final ConsumerModule consumer : config.getConsumers()) {
            final String id = consumer.id() != null ? consumer.id() : consumer.buildId(index++);
            final Key<Consumer> key = Key.get(Consumer.class, Names.named(id));
            modules.add(consumer.module(key, reporter.newConsumer(id)));
            bindings.addBinding().to(key);
        }

        return modules;
    }

    private void startLifeCycles(Injector primary) throws Exception {
        log.info("Starting life cycles");

        if (!awaitLifeCycles("start", primary, 120, LifeCycle::start)) {
            throw new Exception("Failed to start all life cycles");
        }

        log.info("Started all life cycles");
    }

    private void stopLifeCycles(final Injector primary) throws Exception {
        log.info("Stopping life cycles");

        if (!awaitLifeCycles("stop", primary, 10, LifeCycle::stop)) {
            log.warn("Failed to stop all life cycles");
            return;
        }

        log.info("Stopped all life cycles");
    }

    private boolean awaitLifeCycles(final String op, final Injector primary, final int awaitSeconds, final Function<LifeCycle, AsyncFuture<Void>> fn) throws InterruptedException, ExecutionException {
        final AsyncFramework async = primary.getInstance(AsyncFramework.class);

        // we still need to get instances to cause them to be created.
        final Set<LifeCycle> lifeCycles = primary.getInstance(Key.get(new TypeLiteral<Set<LifeCycle>>() {
        }));

        if (skipLifecycles) {
            log.info("{}: skipping (skipLifecycles = true)", op);
            return true;
        }

        final List<AsyncFuture<Void>> futures = new ArrayList<>();
        final List<Pair<AsyncFuture<Void>, LifeCycle>> pairs = new ArrayList<>();

        for (final LifeCycle l : lifeCycles) {
            log.info("{}: running {}", op, l);

            final AsyncFuture<Void> future = fn.apply(l).on(new FutureDone<Void>() {
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
            async.collect(futures).get(awaitSeconds, TimeUnit.SECONDS);
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
        if (profile != null) {
            log.info("Building configuration using profile");
            return profile.build();
        }

        if (configPath == null) {
            log.info("Setting up default configuration");
            return HeroicConfig.builder().build();
        }

        log.info("Loading configuration from {}", configPath.toAbsolutePath());
        return parseConfig(earlyInjector);
    }

    /**
     * Load modules from the specified modules configuration file and wire up those components with early injection.
     *
     * @param injector Injector to wire up modules using.
     * @throws MalformedURLException
     * @throws IOException
     */
    private void loadModules(final Injector injector) throws Exception {
        final List<HeroicModule> modules = new ArrayList<>();

        for (Class<?> builtIn : BUILTIN_MODULES) {
            modules.add(ModuleUtils.loadModule(builtIn));
        }

        for (Class<?> module : this.modules) {
            modules.add(ModuleUtils.loadModule(module));
        }

        for (final HeroicModule entry : modules) {
            log.info("Loading Module: {}", entry.getClass().getPackage().getName());
            // inject members of an entry point and run them.
            injector.injectMembers(entry);
            entry.setup();
        }
    }

    private HeroicConfig parseConfig(final Injector earlyInjector) throws Exception {
        final ObjectMapper mapper = earlyInjector.getInstance(Key.get(ObjectMapper.class,
                Names.named(APPLICATION_HEROIC_CONFIG)));

        try {
            return mapper.readValue(Files.newInputStream(configPath), HeroicConfig.class);
        } catch (final JsonMappingException e) {
            final JsonLocation location = e.getLocation();
            final String message = String.format("%s[%d:%d]: %s", configPath,
                    location == null ? null : location.getLineNr(), location == null ? null : location.getColumnNr(),
                    e.getOriginalMessage());
            throw new Exception(message, e);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String host;
        private Integer port;
        private List<Class<?>> modules = new ArrayList<>();
        private boolean server = false;
        private Path configPath;
        private HeroicProfile profile;
        private HeroicReporter reporter;
        private URI startupPing;
        private String startupId;
        private boolean oneshot = false;
        private boolean disableBackends = false;
        private boolean skipLifecycles = false;
        private List<HeroicBootstrap> bootstrappers = new ArrayList<>();

        public Builder module(Class<?> module) {
            this.modules.add(checkNotNull(module, "module must not be null"));
            return this;
        }

        public Builder modules(Collection<Class<?>> modules) {
            this.modules.addAll(checkNotNull(modules, "list of modules must not be null"));
            return this;
        }

        /**
         * Configure setup of the server component of heroic or not.
         */
        public Builder server(boolean server) {
            this.server = server;
            return this;
        }

        /**
         * Disable local backends.
         */
        public Builder disableBackends(boolean disableBackends) {
            this.disableBackends = disableBackends;
            return this;
        }

        /**
         * Skip startup of lifecycles.
         */
        public Builder skipLifecycles(boolean skipLifecycles) {
            this.skipLifecycles = skipLifecycles;
            return this;
        }

        public Builder port(Integer port) {
            this.port = checkNotNull(port, "port must not be null");
            return this;
        }

        public Builder host(String host) {
            this.host = checkNotNull(host, "host must not be null");
            return this;
        }

        public Builder configPath(String configPath) {
            return configPath(Paths.get(checkNotNull(configPath, "configPath must not be null")));
        }

        public Builder configPath(Path configPath) {
            if (configPath == null) {
                throw new NullPointerException("configPath must not be null");
            }

            if (!Files.isReadable(configPath)) {
                throw new IllegalArgumentException("Configuration is not readable: " + configPath.toAbsolutePath());
            }

            this.configPath = configPath;
            return this;
        }

        public Builder reporter(HeroicReporter reporter) {
            this.reporter = checkNotNull(reporter, "reporter must not be null");
            return this;
        }

        public Builder startupPing(String startupPing) {
            this.startupPing = URI.create(checkNotNull(startupPing, "startupPing must not be null"));
            return this;
        }

        public Builder startupId(String startupId) {
            this.startupId = checkNotNull(startupId, "startupId must not be null");
            return this;
        }

        public Builder profile(HeroicProfile profile) {
            this.profile = checkNotNull(profile, "profile must not be null");
            return this;
        }

        /**
         * Do not perform any scheduled tasks, only perform then once during startup.
         */
        public Builder oneshot(boolean oneshot) {
            this.oneshot = oneshot;
            return this;
        }

        public Builder bootstrap(HeroicBootstrap bootstrap) {
            this.bootstrappers.add(bootstrap);
            return this;
        }

        public HeroicCore build() {
            return new HeroicCore(host, port, modules, bootstrappers, server, configPath, profile, reporter,
                    startupPing, startupId, oneshot, disableBackends, skipLifecycles);
        }

        public Builder modules(List<String> modules) {
            return modules(resolveModuleNames(modules));
        }

        private Collection<Class<?>> resolveModuleNames(List<String> modules) {
            final List<Class<?>> result = new ArrayList<>();

            for (final String module : modules) {
                try {
                    result.add(Class.forName(module + ".Entry"));
                } catch (ClassNotFoundException e) {
                    throw new IllegalArgumentException("Not a valid module name '" + module
                            + "', doesnt have an Entry class");
                }
            }

            return result;
        }
    }
}
