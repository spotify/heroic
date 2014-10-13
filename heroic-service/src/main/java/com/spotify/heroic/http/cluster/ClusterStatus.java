package com.spotify.heroic.http.cluster;

import java.util.List;

import lombok.Data;

import com.spotify.heroic.cluster.ClusterManager;

@Data
public class ClusterStatus {
    private final List<ClusterNodeStatus> nodes;
    private final ClusterManager.Statistics statistics;
}
