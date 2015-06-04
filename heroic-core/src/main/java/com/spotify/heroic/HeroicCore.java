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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Named;
import javax.inject.Singleton;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.name.Names;
import com.spotify.heroic.HeroicInternalLifeCycle.Context;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationQuery;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.aggregation.CoreAggregationRegistry;
import com.spotify.heroic.aggregationcache.AggregationCacheBackendModule;
import com.spotify.heroic.cluster.ClusterDiscoveryModule;
import com.spotify.heroic.cluster.RpcProtocolModule;
import com.spotify.heroic.common.TypeNameMixin;
import com.spotify.heroic.consumer.Consumer;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.filter.CoreFilterFactory;
import com.spotify.heroic.filter.CoreFilterModifier;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.filter.FilterJsonDeserializer;
import com.spotify.heroic.filter.FilterJsonDeserializerImpl;
import com.spotify.heroic.filter.FilterJsonSerializer;
import com.spotify.heroic.filter.FilterJsonSerializerImpl;
import com.spotify.heroic.filter.FilterModifier;
import com.spotify.heroic.filter.FilterSerializer;
import com.spotify.heroic.filter.FilterSerializerImpl;
import com.spotify.heroic.grammar.CoreQueryParser;
import com.spotify.heroic.grammar.QueryParser;
import com.spotify.heroic.injection.CollectingTypeListener;
import com.spotify.heroic.injection.IsSubclassOf;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.model.CacheKeySerializer;
import com.spotify.heroic.model.CacheKeySerializerImpl;
import com.spotify.heroic.model.DataPoint;
import com.spotify.heroic.model.DataPointSerializer;
import com.spotify.heroic.model.Event;
import com.spotify.heroic.model.EventSerializer;
import com.spotify.heroic.model.SamplingSerializer;
import com.spotify.heroic.model.SamplingSerializerImpl;
import com.spotify.heroic.scheduler.DefaultScheduler;
import com.spotify.heroic.scheduler.Scheduler;
import com.spotify.heroic.statistics.HeroicReporter;
import com.spotify.heroic.statistics.noop.NoopHeroicReporter;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.utils.CoreHttpAsyncUtils;
import com.spotify.heroic.utils.HttpAsyncUtils;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.TinyAsync;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;

/**
 * Configure and bootstrap a Heroic application.
 *
 * All public methods are thread-safe.
 *
 * @author udoprog
 */
@Slf4j
public class HeroicCore {
    private static final List<Class<?>> DEFAULT_MODULES = ImmutableList.of();
    private static final List<HeroicConfigurator> DEFAULT_CONFIGURATORS = ImmutableList.of();
    private static final boolean DEFAULT_SERVER = true;
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 8080;
    private static final boolean DEFAULT_ONESHOT = false;

    /**
     * Which resource file to load modules from.
     */
    private static final String APPLICATION_JSON_INTERNAL = "application/json+internal";
    private static final String APPLICATION_JSON = "application/json";
    private static final String APPLICATION_HEROIC_CONFIG = "application/heroic-config";

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
    private final List<HeroicConfigurator> configurators;
    private final boolean server;
    private final Path configPath;
    private final HeroicProfile profile;
    private final HeroicReporter reporter;
    private final URI startupPing;
    private final String startupId;
    private final boolean oneshot;

    public HeroicCore(String host, Integer port, List<Class<?>> modules, List<HeroicConfigurator> configurators,
            Boolean server, Path configPath, HeroicProfile profile, HeroicReporter reporter, URI startupPing,
            String startupId, Boolean oneshot) {
        this.host = Optional.fromNullable(host).or(DEFAULT_HOST);
        this.port = port;
        this.modules = Optional.fromNullable(modules).or(DEFAULT_MODULES);
        this.configurators = Optional.fromNullable(configurators).or(DEFAULT_CONFIGURATORS);
        this.server = Optional.fromNullable(server).or(DEFAULT_SERVER);
        this.configPath = configPath;
        this.profile = profile;
        this.reporter = Optional.fromNullable(reporter).or(NoopHeroicReporter.get());
        this.startupPing = startupPing;
        this.startupId = startupId;
        this.oneshot = Optional.fromNullable(oneshot).or(DEFAULT_ONESHOT);
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

        /* execute configurators (before starting all life cycles) */
        for (final HeroicConfigurator configurator : configurators()) {
            configurator.setup(primary);
        }

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

        this.primary.set(primary);

        lifecycle.start();
        log.info("Heroic was successfully started!");
    }

    /**
     * Inject fields to the provided injectee using the primary injector.
     *
     * @param injectee Object to inject fields on.
     */
    public <T> T inject(T injectee) {
        final Injector primary = this.primary.get();

        if (primary == null) {
            throw new IllegalStateException("heroic has not started");
        }

        primary.injectMembers(injectee);
        return injectee;
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

        final Scheduler scheduler = new DefaultScheduler();

        final ExecutorService executorService = Executors.newFixedThreadPool(1000, new ThreadFactoryBuilder()
                .setNameFormat("heroic-core-%d").build());

        final HeroicInternalLifeCycle lifecycle = new HeroicInernalLifeCycleImpl();

        // build default serialization to be used everywhere.
        final Module serializers = new AbstractModule() {
            @Provides
            @Singleton
            @Named("common")
            private SerializerFramework serializer() {
                return TinySerializer.builder().build();
            }

            @Provides
            @Singleton
            @Inject
            private FilterSerializer filterSerializer(@Named("common") SerializerFramework s) {
                return new FilterSerializerImpl(s, s.integer(), s.string());
            }

            @Provides
            @Singleton
            @Inject
            private CoreAggregationRegistry aggregationRegistry(@Named("common") SerializerFramework s) {
                return new CoreAggregationRegistry(s.string());
            }

            @Provides
            @Singleton
            @Inject
            private AggregationSerializer aggregationSerializer(CoreAggregationRegistry registry) {
                return registry;
            }

            @Provides
            @Singleton
            @Inject
            private AggregationFactory aggregationFactory(CoreAggregationRegistry registry) {
                return registry;
            }

            @Provides
            @Singleton
            @Inject
            private SamplingSerializer samplingSerializer(@Named("common") SerializerFramework s) {
                return new SamplingSerializerImpl(s.longNumber());
            }

            @Provides
            @Singleton
            @Inject
            private CacheKeySerializer cacheKeySerializer(@Named("common") SerializerFramework s,
                    FilterSerializer filter, AggregationSerializer aggregation) {
                return new CacheKeySerializerImpl(s.integer(), filter, s.map(s.nullable(s.string()),
                        s.nullable(s.string())), aggregation, s.longNumber());
            }

            @Override
            protected void configure() {
                bind(FilterJsonSerializer.class).toInstance(new FilterJsonSerializerImpl());
                bind(FilterJsonDeserializer.class).toInstance(new FilterJsonDeserializerImpl());
            }
        };

        final Module core = new AbstractModule() {
            @Provides
            @Singleton
            public AsyncFramework async(ExecutorService executor) {
                return TinyAsync.builder().executor(executor).build();
            }

            @Provides
            @Singleton
            @Named("oneshot")
            private boolean oneshot() {
                return oneshot;
            }

            @Provides
            @Singleton
            @Named(APPLICATION_HEROIC_CONFIG)
            private ObjectMapper configMapper() {
                final ObjectMapper m = new ObjectMapper(new YAMLFactory());

                m.addMixIn(AggregationCacheBackendModule.class, TypeNameMixin.class);
                m.addMixIn(ClusterDiscoveryModule.class, TypeNameMixin.class);
                m.addMixIn(RpcProtocolModule.class, TypeNameMixin.class);
                m.addMixIn(ConsumerModule.class, TypeNameMixin.class);
                m.addMixIn(MetadataModule.class, TypeNameMixin.class);
                m.addMixIn(SuggestModule.class, TypeNameMixin.class);
                m.addMixIn(MetricModule.class, TypeNameMixin.class);

                return m;
            }

            @Override
            protected void configure() {
                bind(Scheduler.class).toInstance(scheduler);
                bind(HeroicInternalLifeCycle.class).toInstance(lifecycle);
                bind(FilterFactory.class).to(CoreFilterFactory.class).in(Scopes.SINGLETON);
                bind(FilterModifier.class).to(CoreFilterModifier.class).in(Scopes.SINGLETON);
                bind(QueryParser.class).to(CoreQueryParser.class).in(Scopes.SINGLETON);

                bind(HeroicConfigurationContext.class).to(CoreHeroicConfigurationContext.class).in(Scopes.SINGLETON);

                bind(HeroicContext.class).toInstance(new CoreHeroicContext());
                bind(ExecutorService.class).toInstance(executorService);

                bind(HttpAsyncUtils.class).toInstance(new CoreHttpAsyncUtils());
            }
        };

        return Guice.createInjector(core, serializers);
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

        final Set<LifeCycle> lifecycles = new HashSet<>();

        final InetSocketAddress bindAddress = setupBindAddress(config);

        // register root components.
        modules.add(new AbstractModule() {
            @Provides
            @Singleton
            public Set<LifeCycle> lifecycles() {
                return lifecycles;
            }

            @Provides
            @Singleton
            @Named("bindAddress")
            public InetSocketAddress bindAddress() {
                return bindAddress;
            }

            @Provides
            @Singleton
            @Named(APPLICATION_JSON_INTERNAL)
            @Inject
            public ObjectMapper internalMapper(FilterJsonSerializer serializer, FilterJsonDeserializer deserializer,
                    AggregationSerializer aggregationSerializer) {
                final SimpleModule module = new SimpleModule("custom");

                final FilterJsonSerializerImpl serializerImpl = (FilterJsonSerializerImpl) serializer;
                final FilterJsonDeserializerImpl deserializerImpl = (FilterJsonDeserializerImpl) deserializer;
                final CoreAggregationRegistry aggregationRegistry = (CoreAggregationRegistry) aggregationSerializer;

                deserializerImpl.configure(module);
                serializerImpl.configure(module);
                aggregationRegistry.configure(module);

                module.addSerializer(DataPoint.class, new DataPointSerializer.Serializer());
                module.addDeserializer(DataPoint.class, new DataPointSerializer.Deserializer());

                module.addSerializer(Event.class, new EventSerializer.Serializer());
                module.addDeserializer(Event.class, new EventSerializer.Deserializer());

                final ObjectMapper mapper = new ObjectMapper();

                mapper.addMixIn(Aggregation.class, TypeNameMixin.class);
                mapper.addMixIn(AggregationQuery.class, TypeNameMixin.class);

                mapper.registerModule(module);
                mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
                return mapper;
            }

            @Provides
            @Singleton
            @Named(APPLICATION_JSON)
            @Inject
            public ObjectMapper jsonMapper(@Named(APPLICATION_JSON_INTERNAL) ObjectMapper mapper) {
                return mapper;
            }

            @Override
            protected void configure() {
                if (server)
                    bind(HeroicServer.class).in(Scopes.SINGLETON);

                bind(HeroicReporter.class).toInstance(reporter);

                if (startupPing != null && startupId != null) {
                    bind(URI.class).annotatedWith(Names.named("startupPing")).toInstance(startupPing);
                    bind(String.class).annotatedWith(Names.named("startupId")).toInstance(startupId);
                    bind(HeroicStartupPinger.class).in(Scopes.SINGLETON);
                }

                bindListener(new IsSubclassOf(LifeCycle.class), new CollectingTypeListener<LifeCycle>(lifecycles));
            }
        });

        modules.add(config.getClient());
        modules.add(config.getMetric());
        modules.add(config.getMetadata());
        modules.add(config.getSuggest());
        modules.add(config.getCluster());
        modules.add(config.getCache());
        modules.add(config.getIngestion());

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

    private List<HeroicConfigurator> configurators() {
        final List<HeroicConfigurator> configurators = new ArrayList<>();
        configurators.addAll(this.configurators);
        return configurators;
    }

    private void startLifeCycles(Injector primary) throws Exception {
        final AsyncFramework async = primary.getInstance(AsyncFramework.class);
        final Set<LifeCycle> lifecycles = primary.getInstance(Key.get(new TypeLiteral<Set<LifeCycle>>() {
        }));

        final List<AsyncFuture<Void>> futures = new ArrayList<>();

        log.info("Starting life cycles");

        for (final LifeCycle startable : lifecycles) {
            log.info("Starting: {}", startable);
            futures.add(startable.start());
        }

        async.collect(futures).get();

        log.info("Started all life cycles");
    }

    private void stopLifeCycles(final Injector primary) throws Exception {
        final AsyncFramework async = primary.getInstance(AsyncFramework.class);
        final Set<LifeCycle> lifecycles = primary.getInstance(Key.get(new TypeLiteral<Set<LifeCycle>>() {
        }));

        final List<AsyncFuture<Void>> futures = new ArrayList<>();
        log.info("Stopping life cycles");

        final List<Pair<AsyncFuture<Void>, LifeCycle>> pairs = new ArrayList<>();

        /* fire Stoppable handlers */
        for (final LifeCycle stoppable : lifecycles) {
            try {
                final AsyncFuture<Void> future = stoppable.stop().on(new FutureDone<Void>() {
                    @Override
                    public void failed(Throwable cause) throws Exception {
                        log.info("Failed to stop: {}", stoppable, cause);
                    }

                    @Override
                    public void resolved(Void result) throws Exception {
                        log.info("Stopped: {}", stoppable);
                    }

                    @Override
                    public void cancelled() throws Exception {
                        log.info("Stop cancelled: {}", stoppable);
                    }
                });

                futures.add(future);
                pairs.add(Pair.of(future, stoppable));
            } catch (Exception e) {
                log.error("Failed to stop {}", stoppable, e);
            }
        }

        try {
            async.collect(futures).get(10, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("Some futures did not stop in a timely fashion!");

            for (final Pair<AsyncFuture<Void>, LifeCycle> pair : pairs) {
                if (!pair.getLeft().isDone())
                    log.error("Did not stop: {}", pair.getRight());
            }
        }

        log.info("Stopped all life cycles");
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

    public static final class Builder {
        private String host;
        private Integer port;
        private List<Class<?>> modules = new ArrayList<>();
        private List<HeroicConfigurator> configurators = new ArrayList<>();
        private boolean server = false;
        private Path configPath;
        private HeroicProfile profile;
        private HeroicReporter reporter;
        private URI startupPing;
        private String startupId;
        private boolean oneshot = false;

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

        public Builder configurator(HeroicConfigurator configurator) {
            this.configurators.add(checkNotNull(configurator, "configurator must not be null"));
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

        public HeroicCore build() {
            return new HeroicCore(host, port, modules, configurators, server, configPath, profile, reporter,
                    startupPing, startupId, oneshot);
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

    public static Builder builder() {
        return new Builder();
    }
}
