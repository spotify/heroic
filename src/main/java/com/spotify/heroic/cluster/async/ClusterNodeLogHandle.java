package com.spotify.heroic.cluster.async;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.Callback;
import com.spotify.heroic.async.CancelReason;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.model.NodeRegistryEntry;

@Slf4j
public final class ClusterNodeLogHandle implements
        Callback.Handle<NodeRegistryEntry> {
    private final ClusterNode clusterNode;

    public ClusterNodeLogHandle(ClusterNode clusterNode) {
        this.clusterNode = clusterNode;
    }

    @Override
    public void cancelled(CancelReason reason) throws Exception {
        log.warn("Cancelled to update metadata for " + clusterNode
                + ". Reason: " + reason);
    }

    @Override
    public void failed(Exception e) throws Exception {
        log.warn("Failed to update metadata for " + clusterNode, e);
    }

    @Override
    public void resolved(NodeRegistryEntry result) throws Exception {

    }
}