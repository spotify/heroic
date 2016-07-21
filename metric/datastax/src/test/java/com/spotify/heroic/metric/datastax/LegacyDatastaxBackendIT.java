package com.spotify.heroic.metric.datastax;

import com.spotify.heroic.metric.datastax.schema.SchemaModule;
import com.spotify.heroic.metric.datastax.schema.legacy.LegacySchemaModule;

import java.util.UUID;

public class LegacyDatastaxBackendIT extends AbstractDatastaxBackendIT {
    @Override
    protected SchemaModule setupSchema(final String keyspace) {
        return LegacySchemaModule.builder().keyspace(keyspace).build();
    }
}
