package com.spotify.heroic.metric;

public interface MetricBackendOperation {
    void run(int disabled, MetricBackend backend) throws Exception;
}