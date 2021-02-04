package com.spotify.heroic;

import static com.spotify.heroic.HeroicConfigurationTestUtils.testConfiguration;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
import com.spotify.heroic.suggest.elasticsearch.SuggestBackendKV;
import com.spotify.heroic.usagetracking.UsageTracking;
import com.spotify.heroic.usagetracking.disabled.DisabledUsageTracking;
import com.spotify.heroic.usagetracking.google.GoogleAnalytics;
import eu.toolchain.async.AsyncFuture;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hamcrest.core.IsInstanceOf;
import org.junit.Test;

public class HeroicConfigurationTest {

    public static final int EXPECTED_NUM_SUGGESTIONS_LIMIT = 100;
    public static final List<String> REFERENCE_STARTERS = ImmutableList.of(
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
    public static final List<String> REFERENCE_STOPPERS = ImmutableList.of(
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

    @Test
    public void testMetricsLimits() throws Exception {
        testConfiguration("heroic-metrics-limits.yml");
    }

    @Test
    public void testHeroicAllYaml() throws Exception {

        final var instance = testConfiguration("heroic-all.yml");

        checkStartersAndStoppers(instance);

        // Check default usage tracking settings
        instance.inject(coreComponent -> {
            UsageTracking tracking = coreComponent.usageTracking();

            IsInstanceOf usageTrackingMatcher = new IsInstanceOf(GoogleAnalytics.class);
            assertTrue(usageTrackingMatcher.matches(tracking));

            return null;
        });

        // Check that the SuggestBackendKV's numSuggestionsLimit was picked up
        // from the heroic-all.yaml config file
        instance.inject(coreComponent -> {
            // First, pluck out the backend from the suggest manager's list of
            // backends
            final var suggestBackendKV =
                (SuggestBackendKV) coreComponent.suggestManager()
                    .groupSet().inspectAll().iterator().next().getMember();

            // Then pluck out the limit we expect to see and verify it
            int limit = suggestBackendKV.getNumSuggestionsLimit().getLimit();
            assertEquals(EXPECTED_NUM_SUGGESTIONS_LIMIT, limit);

            return null;
        });
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

        final HttpContext httpContextClientIdBar = mock(HttpContext.class);
        doReturn(Optional.of("bar")).when(httpContextClientIdBar).getClientId();
        final HttpContext httpContextEmptyClientId = mock(HttpContext.class);
        doReturn(Optional.empty()).when(httpContextEmptyClientId).getClientId();

        final QueryContext queryContextClientIdBar = mock(QueryContext.class);
        doReturn(Optional.of(httpContextClientIdBar)).when(queryContextClientIdBar).httpContext();
        final QueryContext queryContextAnonymous = mock(QueryContext.class);
        doReturn(Optional.of(httpContextEmptyClientId)).when(queryContextAnonymous).httpContext();

        instance.inject(coreComponent -> {
            final ConditionalFeatures conditional = coreComponent.conditionalFeatures().get();

            assertEquals(FeatureSet.of(Feature.CACHE_QUERY),
                conditional.match(queryContextClientIdBar));
            assertEquals(FeatureSet.empty(), conditional.match(queryContextAnonymous));

            return null;
        });
    }

    @Test
    public void testDisabledUsageTracking() throws Exception {
        HeroicCoreInstance instance = testConfiguration("heroic-disabled-tracking.yml");

        instance.inject(coreComponent -> {
            UsageTracking tracking = coreComponent.usageTracking();
            IsInstanceOf matcher = new IsInstanceOf(DisabledUsageTracking.class);

            assertTrue(matcher.matches(tracking));

            return null;
        });
    }

    private static void checkStartersAndStoppers(HeroicCoreInstance instance) throws Exception {
        final List<String> referenceInternalStarters = ImmutableList.of(
            "startup future"
        );

        final List<String> referenceInternalStoppers = ImmutableList.of(
            "loading executor",
            "loading scheduler"
        );

        final List<String> starters = instance.inject(c -> {
            final CoreLifeCycleRegistry reg = (CoreLifeCycleRegistry) c.lifeCycleRegistry();
            return collectLifeCycleNamedHookFutures(reg.starters());
        });

        final List<String> stoppers = instance.inject(c -> {
            final CoreLifeCycleRegistry reg = (CoreLifeCycleRegistry) c.lifeCycleRegistry();
            return collectLifeCycleNamedHookFutures(reg.stoppers());
        });

        assertEquals(REFERENCE_STARTERS, starters);
        assertEquals(REFERENCE_STOPPERS, stoppers);

        final List<String> internalStarters = instance.inject(c -> {
            final CoreLifeCycleRegistry reg = (CoreLifeCycleRegistry) c.internalLifeCycleRegistry();
            return collectLifeCycleNamedHookFutures(reg.starters());
        });

        final List<String> internalStoppers = instance.inject(c -> {
            final CoreLifeCycleRegistry reg = (CoreLifeCycleRegistry) c.internalLifeCycleRegistry();
            return collectLifeCycleNamedHookFutures(reg.stoppers());
        });

        assertEquals(internalStarters, referenceInternalStarters);
        assertEquals(internalStoppers, referenceInternalStoppers);
        return;
    }

    private static List<String> collectLifeCycleNamedHookFutures(
        List<LifeCycleNamedHook<AsyncFuture<Void>>> futures) {
        return futures
            .stream()
            .map(LifeCycleNamedHook::id)
            .sorted()
            .collect(Collectors.toList());
    }
}
