package com.spotify.heroic.metric.bigtable;

import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.test.AbstractMetricBackendIT;
import com.spotify.heroic.test.TestProperties;

import java.util.Optional;

public class BigtableBackendIT extends AbstractMetricBackendIT {
    private final TestProperties properties = TestProperties.ofPrefix("it.bigtable");

    @Override
    public Optional<MetricModule> setupModule() {
        return properties.getOptionalString("remote").map(v -> {
            final String project = properties.getRequiredString("project");
            final String zone = properties.getRequiredString("zone");

            return BigtableMetricModule.builder().project(project).zone(zone).build();
        });
    }
}
