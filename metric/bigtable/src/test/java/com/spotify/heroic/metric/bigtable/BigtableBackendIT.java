package com.spotify.heroic.metric.bigtable;

import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.bigtable.credentials.DefaultCredentialsBuilder;
import com.spotify.heroic.metric.bigtable.credentials.JsonCredentialsBuilder;
import com.spotify.heroic.test.AbstractMetricBackendIT;
import com.spotify.heroic.test.TestProperties;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.UUID;

public class BigtableBackendIT extends AbstractMetricBackendIT {
    private final TestProperties properties = TestProperties.ofPrefix("it.bigtable");

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
    public Optional<MetricModule> setupModule() {
        final String table = "heroic_it_" + UUID.randomUUID();

        final Optional<MetricModule> remote = properties.getOptionalString("remote").map(v -> {
            final String project = properties.getRequiredString("project");
            final String instance = properties.getRequiredString("instance");
            final Optional<String> credentialsFile = properties.getOptionalString("credentials");
            final CredentialsBuilder credentials;

            if (credentialsFile.isPresent()) {
                final Path credentailsPath = Paths.get(credentialsFile.get());
                credentials = JsonCredentialsBuilder.builder().path(credentailsPath).build();
            } else {
                credentials = new DefaultCredentialsBuilder();
            }

            return BigtableMetricModule
                .builder()
                .configure(true)
                .project(project)
                .instance(instance)
                .table(table)
                .credentials(credentials)
                .build();
        });

        if (remote.isPresent()) {
            return remote;
        }

        final BigtableMetricModule module = BigtableMetricModule
            .builder()
            .configure(true)
            .project("fake")
            .table(table)
            .fake(true)
            .build();

        return Optional.of(module);
    }
}
