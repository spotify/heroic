package com.spotify.heroic.analytics.bigtable;

import static org.junit.Assert.assertEquals;

import com.spotify.heroic.HeroicConfigurationTestUtils;
import com.spotify.heroic.dagger.DaggerCoreComponent;
import com.spotify.heroic.metric.LocalMetricManager;
import com.spotify.heroic.metric.MetricsConnectionSettings;
import com.spotify.heroic.metric.bigtable.BigtableBackend;
import com.spotify.heroic.metric.bigtable.BigtableMetricModule;
import com.spotify.heroic.metric.bigtable.MetricsRowKeySerializer;
import eu.toolchain.serializer.TinySerializer;
import java.util.Optional;
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
    public static final int DEFAULT_RETRIES = 2;
    public static final int EXPECTED_MUTATE_RPC_TIMEOUT_MS = 135;
    public static final int EXPECTED_READ_ROWS_RPC_TIMEOUT_MS = 136;
    public static final int EXPECTED_SHORT_RPC_TIMEOUT_MS = 137;
    public static final int EXPECTED_MAX_ELAPSED_BACKOFF_MS = 138;
    public static final int EXPECTED_MAX_SCAN_RETRIES = 7;

    @NotNull
    private static BigtableBackend getBigtableBackend(int maxWriteBatchSize) {
        return getBigtableBackend(maxWriteBatchSize, DEFAULT_TIMEOUT, DEFAULT_TIMEOUT,
                DEFAULT_TIMEOUT, DEFAULT_RETRIES, DEFAULT_TIMEOUT);
    }

    @NotNull
    private static BigtableBackend getBigtableBackend(int maxWriteBatchSize, int mutateRpcTimeoutMs,
                                                      int readRowsRpcTimeoutMs,
                                                      int shortRpcTimeoutMs,
                                                      int maxScanTimeoutRetries,
                                                      int maxElapsedBackoffMs) {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        var serializer = TinySerializer.builder().build();

        var connectionSettings = new MetricsConnectionSettings(Optional.of(maxWriteBatchSize),
        Optional.of(mutateRpcTimeoutMs), Optional.of(readRowsRpcTimeoutMs),
         Optional.of(shortRpcTimeoutMs), Optional.of(maxScanTimeoutRetries),
          Optional.of(maxElapsedBackoffMs));

        var bigtableBackend = new BigtableBackend(null,
                serializer,
                new MetricsRowKeySerializer(),
                null,
                null,
                "bananas",
                false,
                connectionSettings,
                null,
                null);
        return bigtableBackend;
    }

    /**
     * Use this convenience overload if you don't care about *timeoutMs.
     *
     * @param maxWriteBatchSize max size of each written batch of metrics
     * @return a bigtable module object
     */
    private static BigtableMetricModule getBigtableMetricModule(int maxWriteBatchSize) {
        return getBigtableMetricModule(maxWriteBatchSize, DEFAULT_TIMEOUT,
                DEFAULT_TIMEOUT, DEFAULT_TIMEOUT, DEFAULT_RETRIES, DEFAULT_TIMEOUT);
    }

    private static BigtableMetricModule getBigtableMetricModule(
            int maxWriteBatchSize,
            int mutateRpcTimeoutMs,
            int readRowsRpcTimeoutMs,
            int shortRpcTimeoutMs,
            int maxScanTimeoutRetries,
            int maxElapsedBackoffMs) {

        return new BigtableMetricModule.Builder()
                .maxWriteBatchSize(maxWriteBatchSize)
                .mutateRpcTimeoutMs(mutateRpcTimeoutMs)
                .readRowsRpcTimeoutMs(readRowsRpcTimeoutMs)
                .shortRpcTimeoutMs(shortRpcTimeoutMs)
                .maxScanTimeoutRetries(maxScanTimeoutRetries)
                .maxElapsedBackoffMs(maxElapsedBackoffMs)
                .batchSize(1000)
                .project("banana_count")
                .build();
    }

    @Test
    public void testBigtableLimitsConfig() throws Exception {

        final var instance = HeroicConfigurationTestUtils.testConfiguration("heroic-all.yml");

        // Check that the BigTableBackend's maxWriteBatchSize was picked up
        // from the heroic-all.yml config file
        // @formatter:off
        instance.inject(coreComponent -> {
            var metricManager =
                    (LocalMetricManager) ((DaggerCoreComponent) coreComponent)
                            .metricManager();
            var analyticsBackend =
                    metricManager
                            .groupSet()
                            .useGroup("bigtable")
                            .getMembers()
                            .toArray(new BigtableAnalyticsMetricBackend[0])[0];
            var bigtableBackend = (BigtableBackend) analyticsBackend.getBackend();

            // These (int) casts are needed to guide the compiler to pick the correct method
            // call.
            var mcs = bigtableBackend.metricsConnectionSettings();
            assertEquals(EXPECTED_MAX_WRITE_BATCH_SIZE, (int) mcs.getMaxWriteBatchSize());
            assertEquals(EXPECTED_MUTATE_RPC_TIMEOUT_MS, (int) mcs.mutateRpcTimeoutMs);
            assertEquals(EXPECTED_READ_ROWS_RPC_TIMEOUT_MS, (int) mcs.readRowsRpcTimeoutMs);
            assertEquals(EXPECTED_SHORT_RPC_TIMEOUT_MS, (int) mcs.shortRpcTimeoutMs);
            assertEquals(EXPECTED_MAX_SCAN_RETRIES, (int) mcs.maxScanTimeoutRetries);
            assertEquals(EXPECTED_MAX_ELAPSED_BACKOFF_MS, (int) mcs.maxElapsedBackoffMs);

            return null;
        });
        // @formatter:on
    }

    @Test
    public void testMaxWriteBatchSizeLimitsAreEnforced() {
        {
            final int tooBigBatchSize = 5_000_000;

            assertEquals(BigtableMetricModule.MAX_MUTATION_BATCH_SIZE,
                    getBigtableMetricModule(tooBigBatchSize).
                            getMetricsConnectionSettings().
                            getMaxWriteBatchSize());
        }
        {
            final int tooSmallBatchSize = 1;

            assertEquals(BigtableMetricModule.MIN_MUTATION_BATCH_SIZE,
                    getBigtableMetricModule(tooSmallBatchSize).
                            getMetricsConnectionSettings().
                            getMaxWriteBatchSize());
        }
        {
            final int validSize = 100_000;

            assertEquals(validSize,
                    getBigtableMetricModule(validSize).
                            getMetricsConnectionSettings().
                            getMaxWriteBatchSize());
        }
    }
}
