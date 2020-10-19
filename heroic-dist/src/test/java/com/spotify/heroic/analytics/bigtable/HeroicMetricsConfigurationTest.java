package com.spotify.heroic.analytics.bigtable;

import static com.spotify.heroic.HeroicConfigurationTestUtils.testConfiguration;
import static org.junit.Assert.assertEquals;

import com.spotify.heroic.dagger.DaggerCoreComponent;
import com.spotify.heroic.metric.LocalMetricManager;
import com.spotify.heroic.metric.bigtable.BigtableBackend;
import org.junit.Test;

public class HeroicMetricsConfigurationTest {

  public static final int EXPECTED_MAX_WRITE_BATCH_SIZE = 250;

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
}
