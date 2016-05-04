package com.spotify.heroic;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.lifecycle.CoreLifeCycleRegistry;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class HeroicConfigurationTest {
    @Test
    public void testAll() throws Exception {
        // @formatter:off
        final List<String> referenceStarters = ImmutableList.of(
            "com.spotify.heroic.HeroicServer",
            "com.spotify.heroic.analytics.bigtable.BigtableMetricAnalytics",
            "com.spotify.heroic.cluster.CoreClusterManager",
            "com.spotify.heroic.metadata.elasticsearch.MetadataBackendKV",
            "com.spotify.heroic.metadata.elasticsearch.MetadataBackendV1",
            "com.spotify.heroic.metric.astyanax.AstyanaxBackend",
            "com.spotify.heroic.metric.bigtable.BigtableBackend",
            "com.spotify.heroic.metric.datastax.DatastaxBackend",
            "com.spotify.heroic.rpc.nativerpc.NativeRpcProtocolServer",
            "com.spotify.heroic.shell.ShellServer",
            "com.spotify.heroic.suggest.elasticsearch.SuggestBackendKV",
            "com.spotify.heroic.suggest.elasticsearch.SuggestBackendV1"
        );
        // @formatter:on

        // @formatter:off
        final List<String> referenceStoppers = ImmutableList.of(
            "com.spotify.heroic.HeroicServer",
            "com.spotify.heroic.analytics.bigtable.BigtableMetricAnalytics",
            // no stop: com.spotify.heroic.cluster.CoreClusterManager,
            "com.spotify.heroic.metadata.elasticsearch.MetadataBackendKV",
            "com.spotify.heroic.metadata.elasticsearch.MetadataBackendV1",
            "com.spotify.heroic.metric.astyanax.AstyanaxBackend",
            "com.spotify.heroic.metric.bigtable.BigtableBackend",
            "com.spotify.heroic.metric.datastax.DatastaxBackend",
            "com.spotify.heroic.rpc.nativerpc.NativeRpcProtocolServer",
            "com.spotify.heroic.shell.ShellServer",
            "com.spotify.heroic.suggest.elasticsearch.SuggestBackendKV",
            "com.spotify.heroic.suggest.elasticsearch.SuggestBackendV1"
        );
        // @formatter:on

        final HeroicCoreInstance instance = testConfiguration("heroic-all.yml");

        final List<String> starters = instance.inject(c -> {
            final CoreLifeCycleRegistry reg = (CoreLifeCycleRegistry) c.lifeCycleRegistry();
            return reg.starters().stream().map(h -> h.id()).sorted().collect(Collectors.toList());
        });

        final List<String> stoppers = instance.inject(c -> {
            final CoreLifeCycleRegistry reg = (CoreLifeCycleRegistry) c.lifeCycleRegistry();
            return reg.stoppers().stream().map(h -> h.id()).sorted().collect(Collectors.toList());
        });

        assertEquals(referenceStarters, starters);
        assertEquals(referenceStoppers, stoppers);
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
