package com.spotify.heroic.metric.bigtable;

import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.test.AbstractMetricBackendIT;
import java.util.Optional;
import java.util.UUID;
import org.junit.ClassRule;
import org.testcontainers.containers.GenericContainer;

public class BigtableBackendIT extends AbstractMetricBackendIT {
    // TODO reverse emulator image to bigtruedata/gcloud-bigtable-emulator when they fix it
    @ClassRule
    public static GenericContainer container =
        new GenericContainer("malish8632/bigtable-emulator:latest")
            .withExposedPorts(8086)
            .withCommand("start", "--host-port", "0.0.0.0:8086");

    @Override
    protected Optional<Long> period() {
        return Optional.of(BigtableBackend.PERIOD);
    }

    @Override
    protected void setupSupport() {
        super.setupSupport();

        this.eventSupport = true;
        this.maxBatchSize = Optional.of(BigtableBackend.MAX_BATCH_SIZE);
        this.brokenSegmentsPr208 = true;
    }

    @Override
    public MetricModule setupModule() {
        String table = "heroic_it_" + UUID.randomUUID();
        String endpoint = container.getContainerIpAddress() + ":" + container.getFirstMappedPort();

        return BigtableMetricModule
            .builder()
            .configure(true)
            .project("fake")
            .instance("fake")
            .table(table)
            .emulatorEndpoint(endpoint)
            .build();
    }
}
