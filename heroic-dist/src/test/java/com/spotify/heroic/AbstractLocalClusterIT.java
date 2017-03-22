package com.spotify.heroic;

import static org.junit.Assert.assertEquals;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.cluster.RpcProtocolModule;
import com.spotify.heroic.cluster.discovery.simple.StaticListDiscoveryModule;
import com.spotify.heroic.dagger.CoreComponent;
import com.spotify.heroic.profile.MemoryProfile;
import com.spotify.heroic.querylogging.QueryLogger;
import com.spotify.heroic.querylogging.QueryLoggerFactory;
import com.spotify.heroic.querylogging.QueryLoggingComponent;
import com.spotify.heroic.querylogging.QueryLoggingModule;
import com.spotify.heroic.rpc.grpc.GrpcRpcProtocolModule;
import com.spotify.heroic.rpc.jvm.JvmRpcContext;
import com.spotify.heroic.rpc.jvm.JvmRpcProtocolModule;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.TinyAsync;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

public abstract class AbstractLocalClusterIT {
    protected final ExecutorService executor = Executors.newSingleThreadExecutor();
    protected final TinyAsync async = TinyAsync.builder().executor(executor).build();

    protected List<HeroicCoreInstance> instances;

    private final Map<String, QueryLogger> loggers = new HashMap<>();

    private final QueryLoggingModule mockQueryLoggingModule = early -> new QueryLoggingComponent() {
        @Override
        public QueryLoggerFactory queryLoggerFactory() {
            return new QueryLoggerFactory() {
                @Override
                public QueryLogger create(final String component) {
                    synchronized (loggers) {
                        QueryLogger logger = loggers.get(component);

                        if (logger == null) {
                            logger = Mockito.mock(QueryLogger.class);
                            loggers.put(component, logger);
                        }

                        return logger;
                    }
                }
            };
        }
    };

    protected String protocol() {
        return "jvm";
    }

    /**
     * Override to configure more than one instance.
     * <p>
     * These will be available in the {@link #instances} field.
     *
     * @return a list of URIs.
     */
    protected List<URI> instanceUris() {
        return ImmutableList.of(URI.create(protocol() + "://a"), URI.create(protocol() + "://b"));
    }

    /**
     * Prepare the environment before the test.
     * <p>
     * {@link #instances} have been configured before this is called and can be safely used.
     *
     * @return a future that indicates that the environment is ready.
     */
    protected AsyncFuture<Void> prepareEnvironment() {
        return async.resolved(null);
    }

    /**
     * Access to locally pre-configured query loggers which have been provided to all instances.
     *
     * @param component Component to get logger for
     * @return a QueryLogger or empty
     */
    protected Optional<QueryLogger> getQueryLogger(final String component) {
        synchronized (loggers) {
            return Optional.ofNullable(loggers.get(component));
        }
    }

    @Before
    public final void abstractSetup() throws Exception {
        final JvmRpcContext context = new JvmRpcContext();

        final List<URI> uris = instanceUris();
        final List<Integer> expectedNumberOfNodes =
            uris.stream().map(u -> uris.size()).collect(Collectors.toList());

        instances =
            uris.stream().map(uri -> setupCore(uri, uris, context)).collect(Collectors.toList());

        final AsyncFuture<Void> startup = async.collectAndDiscard(
            instances.stream().map(HeroicCoreInstance::start).collect(Collectors.toList()));

        // Refresh all cores, allowing them to discover each other.
        final AsyncFuture<Void> refresh = startup.lazyTransform(v -> {
            return setupStaticNodes().lazyTransform(ignore -> async.collectAndDiscard(instances
                .stream()
                .map(c -> c.inject(inj -> inj.clusterManager().refresh()))
                .collect(Collectors.toList())));
        });

        refresh.lazyTransform(v -> {
            // verify that the correct number of nodes are visible from all instances.
            final List<Integer> actualNumberOfNodes = instances
                .stream()
                .map(c -> c.inject(inj -> inj.clusterManager().getNodes().size()))
                .collect(Collectors.toList());

            assertEquals(expectedNumberOfNodes, actualNumberOfNodes);
            return prepareEnvironment();
        }).get(10, TimeUnit.SECONDS);
    }

    private AsyncFuture<Void> setupStaticNodes() {
        // JVM protocol does not require static nodes since addresses are known beforehand.
        if ("jvm".equals(protocol())) {
            return async.resolved(null);
        }

        // Collect remote URIs from instances to get ahold of generated port.
        final List<AsyncFuture<String>> remotes = instances
            .stream()
            .map(c -> c.inject(CoreComponent::clusterManager))
            .map(c -> c.protocols().iterator().next().getListenURI())
            .collect(Collectors.toList());

        return async.collect(remotes).lazyTransform(uris -> {
            final List<AsyncFuture<Void>> addNodes = new ArrayList<>();

            for (final String stringUri : uris) {
                final URI remoteUri = URI.create(stringUri);

                instances.forEach(instance -> {
                    final URI uri =
                        URI.create(remoteUri.getScheme() + "://127.0.0.1:" + remoteUri.getPort());
                    addNodes.add(instance.inject(CoreComponent::clusterManager).addStaticNode(uri));
                });
            }

            return async.collectAndDiscard(addNodes);
        });
    }

    @After
    public final void abstractTeardown() throws Exception {
        async
            .collectAndDiscard(
                instances.stream().map(HeroicCoreInstance::shutdown).collect(Collectors.toList()))
            .get(10, TimeUnit.SECONDS);
    }

    private HeroicCoreInstance setupCore(
        final URI uri, final List<URI> uris, final JvmRpcContext context
    ) {
        try {
            return setupCoreThrowing(uri, uris, context);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private HeroicCoreInstance setupCoreThrowing(
        final URI uri, final List<URI> uris, final JvmRpcContext context
    ) throws Exception {
        final RpcProtocolModule protocol;
        final StaticListDiscoveryModule discovery;

        switch (uri.getScheme()) {
            case "jvm":
                protocol =
                    JvmRpcProtocolModule.builder().context(context).bindName(uri.getHost()).build();
                discovery = new StaticListDiscoveryModule(uris);
                break;
            case "grpc":
                protocol = GrpcRpcProtocolModule.builder().port(0).build();
                discovery = new StaticListDiscoveryModule(ImmutableList.of());
                break;
            default:
                throw new IllegalArgumentException("Unsupported URI: " + uri);
        }

        return HeroicCore
            .builder()
            .setupShellServer(false)
            .setupService(false)
            .oneshot(true)
            .executor(executor)
            .configFragment(HeroicConfig
                .builder()
                .cluster(ClusterManagerModule
                    .builder()
                    .tags(ImmutableMap.of("shard", uri.getHost()))
                    .protocols(ImmutableList.of(protocol))
                    .discovery(discovery)))
            .configFragment(HeroicConfig.builder().queryLogging(mockQueryLoggingModule))
            .profile(new MemoryProfile())
            .modules(HeroicModules.ALL_MODULES)
            .build()
            .newInstance();
    }
}
