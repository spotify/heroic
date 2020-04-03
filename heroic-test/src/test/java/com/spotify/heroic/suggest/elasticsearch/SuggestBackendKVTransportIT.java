package com.spotify.heroic.suggest.elasticsearch;

import com.spotify.heroic.elasticsearch.ClientWrapper;
import com.spotify.heroic.elasticsearch.TransportClientWrapper;
import java.util.List;

public class SuggestBackendKVTransportIT extends AbstractSuggestBackendKVIT {
    @Override
    protected ClientWrapper setupClient() {
        List<String> seeds = List.of(
            esContainer.getTcpHost().getHostName()
            + ":" + esContainer.getTcpHost().getPort());

        return TransportClientWrapper.builder()
            .clusterName("docker-cluster")
            .seeds(seeds)
            .build();
    }
}
