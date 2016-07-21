package com.spotify.heroic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.cluster.discovery.simple.StaticListDiscoveryModule;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionComponent;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.profile.MemoryProfile;
import com.spotify.heroic.rpc.jvm.JvmRpcContext;
import com.spotify.heroic.rpc.jvm.JvmRpcProtocolModule;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.TinyAsync;
import org.junit.After;
import org.junit.Before;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public abstract class AbstractLocalClusterIT {
    protected final Series s1 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "a"));
    protected final Series s2 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "b"));

    protected final ExecutorService executor = Executors.newSingleThreadExecutor();

    protected final TinyAsync async = TinyAsync.builder().executor(executor).build();

    protected List<HeroicCoreInstance> instances;

    protected QueryManager query;

    @Before
    public final void abstractSetup() throws Exception {
        final JvmRpcContext context = new JvmRpcContext();

        final List<URI> uris = ImmutableList.of(URI.create("jvm://a"), URI.create("jvm://b"));
        final List<Integer> expectedNumberOfNodes =
            uris.stream().map(u -> uris.size()).collect(Collectors.toList());

        instances =
            uris.stream().map(uri -> setupCore(uri, uris, context)).collect(Collectors.toList());

        final AsyncFuture<Void> startup = async.collectAndDiscard(
            instances.stream().map(HeroicCoreInstance::start).collect(Collectors.toList()));

        // Refresh all cores, allowing them to discover each other.
        final AsyncFuture<Void> refresh = startup.lazyTransform(v -> async.collectAndDiscard(
            instances
                .stream()
                .map(c -> c.inject(inj -> inj.clusterManager().refresh()))
                .collect(Collectors.toList())));

        final AsyncFuture<Void> verify = refresh.lazyTransform(v -> {
            // verify that the correct number of nodes are visible from all instances.
            final List<Integer> actualNumberOfNodes = instances
                .stream()
                .map(c -> c.inject(inj -> inj.clusterManager().getNodes().size()))
                .collect(Collectors.toList());

            assertEquals(expectedNumberOfNodes, actualNumberOfNodes);

            final List<IngestionManager> ingestion = instances
                .stream()
                .map(i -> i.inject(IngestionComponent::ingestionManager))
                .collect(Collectors.toList());

            final List<AsyncFuture<Ingestion>> writes = setupWrites(ingestion);

            return async.collectAndDiscard(writes);
        });

        query = verify
            .directTransform(v -> instances.get(0).inject(QueryComponent::queryManager))
            .get(10, TimeUnit.SECONDS);
    }

    protected abstract List<AsyncFuture<Ingestion>> setupWrites(
        final List<IngestionManager> ingestion
    );

    @After
    public final void abstractTeardown() throws Exception {
        async
            .collectAndDiscard(
                instances.stream().map(HeroicCoreInstance::shutdown).collect(Collectors.toList()))
            .get(10, TimeUnit.SECONDS);
    }

    public QueryResult query(final String queryString) throws Exception {
        final Query q = query
            .newQueryFromString(queryString)
            .features(Features.of(Feature.DISTRIBUTED_AGGREGATIONS))
            .source(Optional.of(MetricType.POINT))
            .rangeIfAbsent(Optional.of(new QueryDateRange.Absolute(10, 40)))
            .build();

        return query.useDefaultGroup().query(q).get();
    }

    private HeroicCoreInstance setupCore(
        final URI uri, final List<URI> uris, final JvmRpcContext context
    ) {
        try {
            final JvmRpcProtocolModule protocol =
                JvmRpcProtocolModule.builder().context(context).bindName(uri.getHost()).build();

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
                        .discovery(new StaticListDiscoveryModule(uris))))
                .profile(new MemoryProfile())
                .modules(HeroicModules.ALL_MODULES)
                .build()
                .newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Could not build new instance");
        }
    }
}