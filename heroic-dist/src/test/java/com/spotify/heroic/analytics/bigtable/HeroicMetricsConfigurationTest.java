package com.spotify.heroic.analytics.bigtable;

import static org.junit.Assert.assertEquals;

import com.spotify.heroic.HeroicConfigurationTestUtils;
import com.spotify.heroic.dagger.DaggerCoreComponent;
import com.spotify.heroic.metric.LocalMetricManager;
import com.spotify.heroic.metric.bigtable.BigtableBackend;
import com.spotify.heroic.metric.bigtable.BigtableMetricModule;
import com.spotify.heroic.metric.bigtable.MetricsRowKeySerializer;
import eu.toolchain.serializer.TinySerializer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Tests related to metrics backend configuration in heroic.yml e.g.
 * <pre>
 * metrics:
 *   backends:
 *     - type: bigtable
 *       project: project
 *       maxWriteBatchSize: 250
 * </pre>
 */
@SuppressWarnings({"LineLength"})
public class HeroicMetricsConfigurationTest {

    public static final int EXPECTED_MAX_WRITE_BATCH_SIZE = 250;
    public static final int DEFAULT_TIMEOUT = 250;
    public static final int EXPECTED_MUTATE_RPC_TIMEOUT_MS = 135;
    public static final int EXPECTED_READ_ROWS_RPC_TIMEOUT_MS = 136;
    public static final int EXPECTED_SHORT_RPC_TIMEOUT_MS = 137;

    @NotNull
    private static BigtableBackend getBigtableBackend(int maxWriteBatchSize) {
        return getBigtableBackend(maxWriteBatchSize, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT,
                DEFAULT_TIMEOUT);
    }

    @NotNull
    private static BigtableBackend getBigtableBackend(int maxWriteBatchSize, int mutateRpcTimeoutMs,
                                                      int readRowsRpcTimeoutMs,
                                                      int shortRpcTimeoutMs) {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        var serializer = TinySerializer.builder().build();

        var bigtableBackend = new BigtableBackend(null,
                serializer,
                new MetricsRowKeySerializer(),
                null,
                null,
                "bananas",
                false,
                maxWriteBatchSize,
                mutateRpcTimeoutMs,
                readRowsRpcTimeoutMs,
                shortRpcTimeoutMs,
                null,
                null);
        return bigtableBackend;
    }

    /**
     * Use this convenience overload if you don't care about *timeoutMs.
     * @param maxWriteBatchSize max size of each written batch of metrics
     * @return a bigtable module object
     */
    private static BigtableMetricModule getBigtableMetricModule(int maxWriteBatchSize) {
        return getBigtableMetricModule(maxWriteBatchSize, DEFAULT_TIMEOUT,
                DEFAULT_TIMEOUT, DEFAULT_TIMEOUT);
    }

    private static BigtableMetricModule getBigtableMetricModule(
            int maxWriteBatchSize,
            int mutateRpcTimeoutMs,
            int readRowsRpcTimeoutMs,
            int shortRpcTimeoutMs) {
        return new BigtableMetricModule.Builder()
                .maxWriteBatchSize(maxWriteBatchSize)
                .mutateRpcTimeoutMs(mutateRpcTimeoutMs)
                .readRowsRpcTimeoutMs(readRowsRpcTimeoutMs)
                .shortRpcTimeoutMs(shortRpcTimeoutMs)
                .batchSize(1000)
                .project("banana_count")
                .build();
    }

    @Test
    public void testBigtableLimitsConfig() throws Exception {

        final var instance = HeroicConfigurationTestUtils.testConfiguration("heroic-all.yml");

        // Check that the BigTableBackend's maxWriteBatchSize was picked up
        // from the heroic-all.yml config file
        instance.inject(
            coreComponent -> {
                var metricManager =
                    (LocalMetricManager) ((DaggerCoreComponent) coreComponent).metricManager();
                var analyticsBackend =
                    metricManager
                        .groupSet()
                        .useGroup("bigtable")
                        .getMembers()
                        .toArray(new BigtableAnalyticsMetricBackend[0])[0];
                var bigtableBackend = (BigtableBackend) analyticsBackend.getBackend();

                    assertEquals(EXPECTED_MAX_WRITE_BATCH_SIZE,
                            bigtableBackend.getMaxWriteBatchSize());
                    assertEquals(EXPECTED_MUTATE_RPC_TIMEOUT_MS,
                            bigtableBackend.getMutateRpcTimeoutMs());
                    assertEquals(EXPECTED_READ_ROWS_RPC_TIMEOUT_MS,
                            bigtableBackend.getReadRowsRpcTimeoutMs());
                    assertEquals(EXPECTED_SHORT_RPC_TIMEOUT_MS,
                            bigtableBackend.getShortRpcTimeoutMs());

                    return null;
                });
    }

    @Test
    public void testMaxWriteBatchSizeLimitsAreEnforced() {
        {
            final int tooBigBatchSize = 5_000_000;
            var bigtableBackend = getBigtableMetricModule(tooBigBatchSize);

            assertEquals(BigtableMetricModule.MAX_MUTATION_BATCH_SIZE,
                    bigtableBackend.getMaxWriteBatchSize());
        }
        {
            final int tooSmallBatchSize = 1;
            var bigtableBackend = getBigtableMetricModule(tooSmallBatchSize);

            assertEquals(BigtableMetricModule.MIN_MUTATION_BATCH_SIZE,
                    bigtableBackend.getMaxWriteBatchSize());
        }
        {
            final int validSize = 500_000;
            var bigtableBackend = getBigtableMetricModule(validSize);

            assertEquals(validSize, bigtableBackend.getMaxWriteBatchSize());
        }
    }
}
