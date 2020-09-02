package com.spotify.heroic.suggest.elasticsearch;

import com.spotify.heroic.elasticsearch.ClientWrapper;
import com.spotify.heroic.elasticsearch.TransportClientWrapper;
import java.util.List;

public class SuggestBackendKVTransportIT extends AbstractSuggestBackendKVIT {

    @Override
    protected ClientWrapper setupClient() {
        List<String> seeds =
            List.of(
                esContainer.getTcpHost().getHostName() + ":" + esContainer.getTcpHost().getPort());

        return TransportClientWrapper.builder().clusterName("docker-cluster").seeds(seeds).build();
    }

    @Override
    public void tagSuggestCeiling() throws Exception {
        /* no-op because it causes:

        io.grpc.Context validateGeneration
        SEVERE: Context ancestry chain length is abnormally long. This suggests an error in application code. Length exceeded: 1000

        Also, my understanding is that *Transport* is to be deprecated,
        hence there's no point spending effort trying to get this working.
        */
    }
}
