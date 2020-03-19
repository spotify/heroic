package com.spotify.heroic.suggest.elasticsearch;

import static org.junit.Assert.assertEquals;

import com.spotify.heroic.elasticsearch.ConnectionModule;
import com.spotify.heroic.elasticsearch.TransportClientSetup;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.suggest.WriteSuggest;
import com.spotify.heroic.test.ElasticSearchTestContainer;
import com.spotify.heroic.test.AbstractSuggestBackendIT;
import java.util.List;
import java.util.UUID;
import org.junit.Test;

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
            .writeCacheMaxSize(0)
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


    @Test
    public void writeDuplicatesReturnErrorInResponse() throws Exception {
        final WriteSuggest firstWrite =
            backend.write(new WriteSuggest.Request(testSeries.get(0).getKey(), range)).get();
        final WriteSuggest secondWrite =
            backend.write(new WriteSuggest.Request(testSeries.get(0).getKey(), range)).get();

        assertEquals(0, firstWrite.getErrors().size());
        assertEquals(2, secondWrite.getErrors().size());
    }

}
