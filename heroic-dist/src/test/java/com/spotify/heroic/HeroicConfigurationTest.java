package com.spotify.heroic;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.conditionalfeatures.ConditionalFeatures;
import com.spotify.heroic.lifecycle.CoreLifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycleNamedHook;
import com.spotify.heroic.querylogging.HttpContext;
import com.spotify.heroic.querylogging.QueryContext;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.Test;

public class HeroicConfigurationTest {
    @Test
    public void testMetricsLimits() throws Exception {
        testConfiguration("heroic-metrics-limits.yml");
    }

    @Test
    public void testAll() throws Exception {
        // @formatter:off
        final List<String> referenceStarters = ImmutableList.of(
            "com.spotify.heroic.analytics.bigtable.BigtableMetricAnalytics",
            "com.spotify.heroic.cluster.CoreClusterManager",
            "com.spotify.heroic.consumer.kafka.KafkaConsumer",
            "com.spotify.heroic.http.HttpServer",
            "com.spotify.heroic.metadata.elasticsearch.MetadataBackendKV",
            "com.spotify.heroic.metric.bigtable.BigtableBackend",
            "com.spotify.heroic.metric.datastax.DatastaxBackend",
            "com.spotify.heroic.rpc.grpc.GrpcRpcProtocolServer",
            "com.spotify.heroic.shell.ShellServer",
            "com.spotify.heroic.suggest.elasticsearch.SuggestBackendKV"
        );
        // @formatter:on

        // @formatter:off
        final List<String> referenceStoppers = ImmutableList.of(
            "com.spotify.heroic.analytics.bigtable.BigtableMetricAnalytics",
            "com.spotify.heroic.cluster.CoreClusterManager",
            "com.spotify.heroic.consumer.kafka.KafkaConsumer",
            "com.spotify.heroic.http.HttpServer",
            "com.spotify.heroic.metadata.elasticsearch.MetadataBackendKV",
            "com.spotify.heroic.metric.bigtable.BigtableBackend",
            "com.spotify.heroic.metric.datastax.DatastaxBackend",
            "com.spotify.heroic.rpc.grpc.GrpcRpcProtocolServer",
            "com.spotify.heroic.shell.ShellServer",
            "com.spotify.heroic.suggest.elasticsearch.SuggestBackendKV"
        );
        // @formatter:on

        // @formatter:off
        final List<String> referenceInternalStarters = ImmutableList.of(
            "startup future"
        );
        // @formatter:on

        // @formatter:off
        final List<String> referenceInternalStoppers = ImmutableList.of(
            "loading executor",
            "loading scheduler"
        );
        // @formatter:on

        final HeroicCoreInstance instance = testConfiguration("heroic-all.yml");

        final List<String> starters = instance.inject(c -> {
            final CoreLifeCycleRegistry reg = (CoreLifeCycleRegistry) c.lifeCycleRegistry();
            return reg
                .starters()
                .stream()
                .map(LifeCycleNamedHook::id)
                .sorted()
                .collect(Collectors.toList());
        });

        final List<String> stoppers = instance.inject(c -> {
            final CoreLifeCycleRegistry reg = (CoreLifeCycleRegistry) c.lifeCycleRegistry();
            return reg
                .stoppers()
                .stream()
                .map(LifeCycleNamedHook::id)
                .sorted()
                .collect(Collectors.toList());
        });

        assertEquals(referenceStarters, starters);
        assertEquals(referenceStoppers, stoppers);

        final List<String> internalStarters = instance.inject(c -> {
            final CoreLifeCycleRegistry reg = (CoreLifeCycleRegistry) c.internalLifeCycleRegistry();
            return reg
                .starters()
                .stream()
                .map(LifeCycleNamedHook::id)
                .sorted()
                .collect(Collectors.toList());
        });

        final List<String> internalStoppers = instance.inject(c -> {
            final CoreLifeCycleRegistry reg = (CoreLifeCycleRegistry) c.internalLifeCycleRegistry();
            return reg
                .stoppers()
                .stream()
                .map(LifeCycleNamedHook::id)
                .sorted()
                .collect(Collectors.toList());
        });

        assertEquals(internalStarters, referenceInternalStarters);
        assertEquals(internalStoppers, referenceInternalStoppers);
    }

    @Test
    public void testNullShellHost() throws Exception {
        // TODO: get this into the shell server module
        final HeroicCoreInstance instance = testConfiguration("heroic-null-shell-host.yml");
    }

    @Test
    public void testQueryLoggingConfiguration() throws Exception {
        final HeroicCoreInstance instance = testConfiguration("heroic-query-logging.yml");
    }

    @Test
    public void testKafkaConfiguration() throws Exception {
        final HeroicCoreInstance instance = testConfiguration("heroic-kafka.yml");
        instance.inject(coreComponent -> {
            assertEquals(coreComponent.consumers().size(), 1);
            assertEquals(coreComponent.consumers().iterator().next().getClass(),
                com.spotify.heroic.consumer.kafka.KafkaConsumer.class);
            return null;
        });
    }

    @Test
    public void testDatastaxConfiguration() throws Exception {
        testConfiguration("heroic-datastax.yml");
    }

    /**
     * Test that conditional features from configuration can be parsed and used.
     */
    @Test
    public void testConditionalFeatures() throws Exception {
        final HeroicCoreInstance instance = testConfiguration("heroic-conditional-features.yml");

        final HttpContext httpContext1 = mock(HttpContext.class);
        doReturn(Optional.of("bar")).when(httpContext1).getClientId();
        final HttpContext httpContext2 = mock(HttpContext.class);
        doReturn(Optional.empty()).when(httpContext2).getClientId();

        final QueryContext context1 = mock(QueryContext.class);
        doReturn(Optional.of(httpContext1)).when(context1).getHttpContext();
        final QueryContext context2 = mock(QueryContext.class);
        doReturn(Optional.of(httpContext2)).when(context2).getHttpContext();

        instance.inject(coreComponent -> {
            final ConditionalFeatures conditional = coreComponent.conditionalFeatures().get();

            assertEquals(FeatureSet.of(Feature.CACHE_QUERY),
                conditional.match(context1));
            assertEquals(FeatureSet.empty(), conditional.match(context2));

            return null;
        });
    }

    private HeroicCoreInstance testConfiguration(final String name) throws Exception {
        final HeroicCore.Builder builder = HeroicCore.builder();
        builder.modules(HeroicModules.ALL_MODULES);
        builder.configStream(stream(name));
        return builder.build().newInstance();
    }

    private Supplier<InputStream> stream(String name) {
        return () -> getClass().getClassLoader().getResourceAsStream(name);
    }
}
