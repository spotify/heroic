package com.spotify.heroic.metric.datastax;

import com.spotify.heroic.metric.datastax.schema.SchemaModule;
import com.spotify.heroic.metric.datastax.schema.ng.NextGenSchemaModule;

public class NextGenDatastaxBackendIT extends AbstractDatastaxBackendIT {
    @Override
    protected SchemaModule setupSchema(final String keyspace) {
        return NextGenSchemaModule.builder().keyspace(keyspace).build();
    }
}
