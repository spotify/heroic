package com.spotify.heroic.metrics;

public interface BackendOperation {
    void run(int disabled, Backend backend) throws Exception;
}