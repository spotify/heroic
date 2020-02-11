package com.spotify.heroic.metric.datastax;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.datastax.schema.SchemaModule;
import com.spotify.heroic.test.AbstractMetricBackendIT;
import java.util.UUID;
import org.junit.ClassRule;
import org.testcontainers.containers.CassandraContainer;

public abstract class AbstractDatastaxBackendIT extends AbstractMetricBackendIT {
    @ClassRule
    public static CassandraContainer container = new CassandraContainer();

    @Override
    protected MetricModule setupModule() {
        final String keyspace = "heroic_it_" + UUID.randomUUID().toString().replace('-', '_');
        final String seed = container.getContainerIpAddress() + ":" + container.getFirstMappedPort();

        return DatastaxMetricModule.builder()
            .schema(setupSchema(keyspace))
            .configure(true)
            .seeds(ImmutableSet.of(seed))
            .build();
    }

    abstract protected SchemaModule setupSchema(final String keyspace);
}
