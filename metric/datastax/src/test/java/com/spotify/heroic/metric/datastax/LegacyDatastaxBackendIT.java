package com.spotify.heroic.metric.datastax;

import com.spotify.heroic.metric.datastax.schema.SchemaModule;
import com.spotify.heroic.metric.datastax.schema.legacy.LegacySchemaInstance;
import com.spotify.heroic.metric.datastax.schema.legacy.LegacySchemaModule;

import java.util.Optional;

public class LegacyDatastaxBackendIT extends AbstractDatastaxBackendIT {
    @Override
    protected Optional<Long> period() {
        return Optional.of(LegacySchemaInstance.MAX_WIDTH);
    }

    @Override
    protected SchemaModule setupSchema(final String keyspace) {
        return LegacySchemaModule.builder().keyspace(keyspace).build();
    }

    @Override
    protected void setupSupport() {
        super.setupSupport();

        this.brokenSegmentsPr208 = true;
    }
}
