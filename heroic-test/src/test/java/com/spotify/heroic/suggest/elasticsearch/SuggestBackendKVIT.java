package com.spotify.heroic.suggest.elasticsearch;

import com.spotify.heroic.elasticsearch.ConnectionModule;
import com.spotify.heroic.elasticsearch.TransportClientSetup;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.test.ElasticSearchTestContainer;
import com.spotify.heroic.test.AbstractSuggestBackendIT;
import java.util.List;
import java.util.UUID;

public class SuggestBackendKVIT extends AbstractSuggestBackendIT {
    private final static ElasticSearchTestContainer esContainer;

    static {
        esContainer = ElasticSearchTestContainer.getInstance();
    }

    private String backendType() {
        return "kv";
    }

    @Override
    protected SuggestModule setupModule() throws Exception {
        final String testName = "heroic-it-" + UUID.randomUUID().toString();

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
                .clientSetup(TransportClientSetup.builder()
                    .clusterName("docker-cluster")
                    .seeds(List.of(
                        esContainer.getTcpHost().getHostName()
                        + ":" + esContainer.getTcpHost().getPort()))
                    .build())
                .build())
            .build();
    }
}
