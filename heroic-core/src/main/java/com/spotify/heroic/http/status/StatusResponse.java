package com.spotify.heroic.http.status;

import lombok.Data;

@Data
public class StatusResponse {
    @Data
    public static class Consumer {
        private final boolean ok;
        private final int available;
        private final int ready;

        /**
         * Total number of consumer errors encountered.
         */
        private final long errors;
        private final long consumingThreads;
        private final long totalThreads;
    }

    @Data
    public static class Backend {
        private final boolean ok;
        private final int available;
        private final int ready;
    }

    @Data
    public static class MetadataBackend {
        private final boolean ok;
        private final int available;
        private final int ready;
    }

    @Data
    public static class Cluster {
        private final boolean ok;
        private final int onlineNodes;
        private final int offlineNodes;
    }

    private final boolean ok;
    private final Consumer consumers;
    private final Backend backends;
    private final MetadataBackend metadataBackends;
    private final Cluster cluster;
}
