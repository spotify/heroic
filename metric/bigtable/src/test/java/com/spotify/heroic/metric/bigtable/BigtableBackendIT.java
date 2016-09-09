package com.spotify.heroic.metric.bigtable;

import com.spotify.heroic.metric.MetricModule;
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
    public Optional<MetricModule> setupModule() {
        return properties.getOptionalString("remote").map(v -> {
            final String table = "heroic_it_" + UUID.randomUUID();

            final String project = properties.getRequiredString("project");
            final String instance = properties.getRequiredString("instance");
            final Path credentials = Paths.get(properties.getRequiredString("credentials"));

            return BigtableMetricModule
                .builder()
                .configure(true)
                .project(project)
                .instance(instance)
                .table(table)
                .credentials(JsonCredentialsBuilder.builder().path(credentials).build())
                .build();
        });
    }
}
