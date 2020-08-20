package com.spotify.heroic.metric.datastax;

import com.spotify.heroic.metric.datastax.schema.SchemaModule;
import com.spotify.heroic.metric.datastax.schema.ng.NextGenSchemaInstance;
import com.spotify.heroic.metric.datastax.schema.ng.NextGenSchemaModule;
import java.util.Optional;

public class NextGenDatastaxBackendIT extends AbstractDatastaxBackendIT {
    @Override
    protected Optional<Long> period() {
        return Optional.of(NextGenSchemaInstance.MAX_WIDTH);
    }

    @Override
    protected SchemaModule setupSchema(final String keyspace) {
        return NextGenSchemaModule.builder().keyspace(keyspace).build();
    }

    @Override
    protected void setupSupport() {
        super.setupSupport();

        this.brokenSegmentsPr208 = true;
        this.hugeRowKey = false;
    }
}
