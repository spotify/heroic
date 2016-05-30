package com.spotify.heroic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.cluster.ClusterComponent;
import com.spotify.heroic.cluster.ClusterManager;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.cluster.discovery.simple.StaticListDiscoveryModule;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.ingestion.Ingestion;
import com.spotify.heroic.ingestion.IngestionComponent;
import com.spotify.heroic.ingestion.IngestionManager;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.ShardedResultGroup;
import com.spotify.heroic.profile.MemoryProfile;
import com.spotify.heroic.rpc.jvm.JvmRpcContext;
import com.spotify.heroic.rpc.jvm.JvmRpcProtocolModule;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.TinyAsync;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static com.spotify.heroic.test.Data.points;
import static org.junit.Assert.assertEquals;

public class ClusterIT {
    private final Series s1 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "a"));
    private final Series s2 = Series.of("key1", ImmutableMap.of("shared", "a", "diff", "b"));

    private final TinyAsync async =
        TinyAsync.builder().executor(Executors.newSingleThreadExecutor()).build();

    private List<HeroicCoreInstance> instances;

    private QueryManager query;
    private ClusterManager cluster;

    private IngestionManager m1;
    private IngestionManager m2;

    @Before
    public void setup() throws Exception {
        final JvmRpcContext context = new JvmRpcContext();

        final List<URI> uris = ImmutableList.of(URI.create("jvm://a"), URI.create("jvm://b"));
        final List<Integer> expectedNumberOfNodes =
            uris.stream().map(u -> uris.size()).collect(Collectors.toList());

        instances =
            uris.stream().map(uri -> setupCore(uri, uris, context)).collect(Collectors.toList());

        async
            .collectAndDiscard(
                instances.stream().map(HeroicCoreInstance::start).collect(Collectors.toList()))
            .get();

        // Refresh all cores, allowing them to discover each other.
        async
            .collectAndDiscard(instances
                .stream()
                .map(c -> c.inject(inj -> inj.clusterManager().refresh()))
                .collect(Collectors.toList()))
            .get();

        final List<Integer> actualNumberOfNodes = instances
            .stream()
            .map(c -> c.inject(inj -> inj.clusterManager().getNodes().size()))
            .collect(Collectors.toList());

        assertEquals(expectedNumberOfNodes, actualNumberOfNodes);

        query = instances.get(0).inject(QueryComponent::queryManager);
        cluster = instances.get(0).inject(ClusterComponent::clusterManager);

        m1 = instances.get(0).inject(IngestionComponent::ingestionManager);
        m2 = instances.get(1).inject(IngestionComponent::ingestionManager);

        final List<AsyncFuture<Ingestion>> writes = new ArrayList<>();

        writes.add(m1
            .useDefaultGroup()
            .write(new Ingestion.Request(s1, points().p(0, 1D).p(20, 2D).build())));
        writes.add(m2
            .useDefaultGroup()
            .write(new Ingestion.Request(s2, points().p(0, 3D).p(10, 4D).build())));

        async.collectAndDiscard(writes).get();
    }

    @After
    public void teardown() throws Exception {
        async
            .collectAndDiscard(
                instances.stream().map(HeroicCoreInstance::shutdown).collect(Collectors.toList()))
            .get();
    }

    @Test
    public void basicQueryTest() throws Exception {
        final QueryResult result = query("sum(10ms) from points(0, 10000)");

        final Set<MetricCollection> m = result
            .getGroups()
            .stream()
            .map(ShardedResultGroup::getMetrics)
            .collect(Collectors.toSet());

        assertEquals(ImmutableSet.of(points().p(0, 4D).p(10, 4D).p(20, 2D).build()), m);
    }

    @Test
    public void distributedQueryTest() throws Exception {
        final QueryResult result = query("sum(10ms) by shared from points(0, 10000)");

        final Set<MetricCollection> m = result
            .getGroups()
            .stream()
            .map(ShardedResultGroup::getMetrics)
            .collect(Collectors.toSet());

        assertEquals(ImmutableSet.of(points().p(0, 4D).p(10, 4D).p(20, 2D).build()), m);
    }

    @Test
    public void distributedDifferentQueryTest() throws Exception {
        final QueryResult result = query("sum(10ms) by diff from points(0, 10000)");

        final Set<MetricCollection> m = result
            .getGroups()
            .stream()
            .map(ShardedResultGroup::getMetrics)
            .collect(Collectors.toSet());

        assertEquals(ImmutableSet.of(points().p(0, 1D).p(20, 2D).build(),
            points().p(0, 3D).p(10, 4D).build()), m);
    }

    public QueryResult query(final String queryString) throws Exception {
        final Query q = query
            .newQueryFromString(queryString)
            .features(Features.of(Feature.DISTRIBUTED_AGGREGATIONS))
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
