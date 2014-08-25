package com.spotify.heroic.http.model.status;

import lombok.Data;

@Data
public class StatusResponse {
    private final boolean ok;
    private final ConsumerStatusResponse consumers;
    private final BackendStatusResponse backends;
    private final MetadataBackendStatusResponse metadataBackends;
    private final ClusterStatusResponse cluster;
}
