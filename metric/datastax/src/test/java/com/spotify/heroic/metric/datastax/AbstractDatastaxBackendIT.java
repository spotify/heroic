package com.spotify.heroic.metric.datastax;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.datastax.schema.SchemaModule;
import com.spotify.heroic.test.AbstractMetricBackendIT;
import com.spotify.heroic.test.TestProperties;

import java.util.Optional;
import java.util.UUID;

public abstract class AbstractDatastaxBackendIT extends AbstractMetricBackendIT {
    private final TestProperties properties = TestProperties.ofPrefix("it.datastax");

    @Override
    protected Optional<MetricModule> setupModule() {
        return properties.getOptionalString("remote").map(v -> {
            final String seed = properties.getRequiredString("seed");

            final String keyspace = "heroic_it_" + UUID.randomUUID().toString().replace('-', '_');

            return DatastaxMetricModule
                .builder()
                .schema(setupSchema(keyspace))
                .configure(true)
                .seeds(ImmutableSet.of(seed))
                .build();
        });
    }

    abstract protected SchemaModule setupSchema(final String keyspace);
}
