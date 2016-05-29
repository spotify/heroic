package com.spotify.heroic.suggest.elasticsearch;

import com.spotify.heroic.elasticsearch.ConnectionModule;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.test.AbstractSuggestBackendIT;
import com.spotify.heroic.test.ElasticsearchTestUtils;

import java.util.UUID;

public abstract class AbstractElasticsearchSuggestBackendIT extends AbstractSuggestBackendIT {
    private final String testName = "heroic-it-" + UUID.randomUUID().toString();

    protected abstract String backendType();

    @Override
    protected SuggestModule setupModule() throws Exception {
        final RotatingIndexMapping index =
            RotatingIndexMapping.builder().pattern(testName + "-%s").build();

        return ElasticsearchSuggestModule
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
