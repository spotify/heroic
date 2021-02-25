package com.spotify.heroic.metric.bigtable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.spotify.heroic.HeroicCoreInstance;
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.metric.FetchData;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.test.AbstractMetricBackendIT;
import com.spotify.heroic.test.Points;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

public class BigtableBackendIT extends AbstractMetricBackendIT {
    public static final int VERY_SHORT_WAIT_TIME_MS = 10;
    // TODO revert emulator image to bigtruedata/gcloud-bigtable-emulator when they fix it
    @ClassRule
    public static GenericContainer emulatorContainer =
        new GenericContainer("malish8632/bigtable-emulator:latest")
            .withExposedPorts(8086)
            .withCommand("start", "--host-port", "0.0.0.0:8086");

    /**
     * This is an image of adamsteele@google.com's personal fork of the Bigtable
     * emulator that respects request timeout configuration limits. When it is
     * merged, we can get rid of this and just use malish8632/bigtable-emulator:latest
     */
    @ClassRule
    public static GenericContainer slowEmulatorContainer =
        new GenericContainer("us.gcr.io/bigtable-playground-272916/btemu:latest")
            .withExposedPorts(9000)
            .withCommand("--host=0.0.0.0 --port=9000 --inject-latency=ReadRows:p00:5m");

    @Override
    protected Optional<Long> period() {
        return Optional.of(BigtableBackend.PERIOD);
    }

    /**
     * This test is marginally useful because it is testing the Bigtable emulator
     * and not the real thing. It also doesn't test that the user experiences timeouts
     * correctly.
     *
     * It instead just tests that we will get the exceptions that our metrics code
     * expects (see SemanticMetricBackendReporter for timeout counting methods)
     * and more importantly that the Bigtable client doesn't change what it throws
     * upon a timeout.
     */
    @Test
    public void testBackendTimesOutCorrectly() throws Exception {

        // We write out our data outside the try..catch because writes aren't the
        // system under test.
        var numPoints = 100;
        var startTimestamp = 100000L;
        var mc = new Points().p(startTimestamp, MEANING_OF_LIFE_HHGTTG, numPoints).build();
        backend.write(new WriteMetric.Request(s3, mc)).get();

        // Now we basically emulate AbstractMetricBackendIT.setupBackend except we create
        // a special core with very short Bigtable read timeouts, which will be hit when
        // we do our large fetch operation
        final HeroicCoreInstance core =
                getHeroicCoreInstance(BackendModuleMode.VERY_SHORT_TIMEOUTS);

        // This is what gives us the special VERY_SHORT_TIMEOUTS behaviour.
        backend = createBackend(core);

        try {
            var request =
                    new FetchData.Request(MetricType.POINT, s3,
                            new DateRange(0, startTimestamp + numPoints),
                            QueryOptions.builder().build());
            fetchMetrics(request, true);
        } catch (ExecutionException  tertiaryCause) {
            var secondaryCause = tertiaryCause.getCause();
            assertEquals(secondaryCause.getClass(),
                    com.google.cloud.bigtable.grpc.scanner.BigtableRetriesExhaustedException.class);
            assertTrue(secondaryCause.getMessage().startsWith("Exhausted retries after "));

            var primaryCause = secondaryCause.getCause();
            assertEquals(primaryCause.getClass(), io.grpc.StatusRuntimeException.class);

            assertTrue("The primary cause: " + primaryCause.getMessage(), primaryCause.getMessage()
                    .contains("DEADLINE_EXCEEDED: deadline exceeded"));
        } finally {
            core.shutdown().get();
        }
    }

    @Override
    protected void setupSupport() {
        super.setupSupport();

        this.eventSupport = true;
        this.maxBatchSize = BigtableMetricModule.DEFAULT_MUTATION_BATCH_SIZE;
        this.brokenSegmentsPr208 = true;
    }

    @Override
    public MetricModule setupModule(BackendModuleMode mode) {
        String table = "heroic_it_" + UUID.randomUUID();

        var container = mode == BackendModuleMode.NORMAL
                ? emulatorContainer
                : slowEmulatorContainer;

        String endpoint =
                container.getContainerIpAddress() + ":" + container.getFirstMappedPort();

        var moduleBuilder = BigtableMetricModule
                .builder()
                .configure(true)
                .project("fake")
                .instance("fake")
                .table(table)
                .emulatorEndpoint(endpoint);

        if (mode == BackendModuleMode.VERY_SHORT_TIMEOUTS) {
            moduleBuilder.maxElapsedBackoffMs(VERY_SHORT_WAIT_TIME_MS)
                    .readRowsRpcTimeoutMs(VERY_SHORT_WAIT_TIME_MS)
                    .shortRpcTimeoutMs(VERY_SHORT_WAIT_TIME_MS);
        }

        return moduleBuilder.build();
    }
}
