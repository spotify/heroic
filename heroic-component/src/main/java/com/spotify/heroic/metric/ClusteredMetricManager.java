package com.spotify.heroic.metric;

import com.spotify.heroic.cluster.model.NodeRegistryEntry;

import eu.toolchain.async.AsyncFuture;

public interface ClusteredMetricManager {
    public AsyncFuture<MetricResult> queryOnNode(MetricQuery request, NodeRegistryEntry node);

    public AsyncFuture<MetricResult> query(MetricQuery request);

    public MetricQueryBuilder newRequest();
}