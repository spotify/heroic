package com.spotify.heroic.metadata.elasticsearch;

import com.spotify.heroic.elasticsearch.ConnectionModule;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.elasticsearch.test.ElasticsearchTestUtils;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.test.AbstractMetadataBackendIT;
import java.util.UUID;

public abstract class AbstractElasticsearchMetadataBackendIT extends AbstractMetadataBackendIT {
    protected abstract String backendType();

    @Override
    protected MetadataModule setupModule() throws Exception {
        final String testName = "heroic-it-" + UUID.randomUUID().toString();

        final RotatingIndexMapping index =
            RotatingIndexMapping.builder().pattern(testName + "-%s").build();

        return ElasticsearchMetadataModule
            .builder()
            .templateName(testName)
            .configure(true)
            .backendType(backendType())
            .connection(ConnectionModule
                .builder()
                .index(index)
                .clientSetup(ElasticsearchTestUtils.clientSetup())
                .build())
            .build();
    }
}
