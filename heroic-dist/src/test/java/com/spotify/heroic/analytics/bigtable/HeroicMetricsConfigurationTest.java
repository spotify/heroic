package com.spotify.heroic.analytics.bigtable;

import static com.spotify.heroic.HeroicConfigurationTestUtils.testConfiguration;
import static org.junit.Assert.assertEquals;

import com.spotify.heroic.dagger.DaggerCoreComponent;
import com.spotify.heroic.metric.LocalMetricManager;
import com.spotify.heroic.metric.bigtable.BigtableBackend;
import com.spotify.heroic.metric.bigtable.BigtableMetricModule;
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
public class HeroicMetricsConfigurationTest {

    public static final int EXPECTED_MAX_WRITE_BATCH_SIZE = 250;

    private static BigtableMetricModule getBigtableMetricModule(int maxWriteBatchSize) {
        return new BigtableMetricModule.Builder()
            .maxWriteBatchSize(maxWriteBatchSize)
            .batchSize(1000)
            .project("banana_count")
            .build();
    }

    @Test
    public void testMandatoryClientIdWarning() throws Exception {
        final var instance = testConfiguration("heroic-all.yml");
        // TODO ...
    }

    @Test
    public void testMaxWriteBatchSizeConfig() throws Exception {

        final var instance = testConfiguration("heroic-all.yml");

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

                assertEquals(EXPECTED_MAX_WRITE_BATCH_SIZE, bigtableBackend.getMaxWriteBatchSize());

                return null;
            });
    }

    @Test
    public void testMaxWriteBatchSizeLimitsAreEnforced() {
        {
            final int tooBigBatchSize = 5_000_000;
            var bigtableBackend = getBigtableMetricModule(tooBigBatchSize);

            assertEquals(BigtableMetricModule.MAX_MUTATION_BATCH_SIZE, bigtableBackend.getMaxWriteBatchSize());
        }
        {
            final int tooSmallBatchSize = 1;
            var bigtableBackend = getBigtableMetricModule(tooSmallBatchSize);

            assertEquals(BigtableMetricModule.MIN_MUTATION_BATCH_SIZE, bigtableBackend.getMaxWriteBatchSize());
        }
        {
            final int validSize = 500_000;
            var bigtableBackend = getBigtableMetricModule(validSize);

            assertEquals(validSize, bigtableBackend.getMaxWriteBatchSize());
        }
    }
}
