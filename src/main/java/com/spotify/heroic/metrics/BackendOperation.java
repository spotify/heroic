package com.spotify.heroic.metrics;

interface BackendOperation {
    void run(int disabled, Backend backend) throws Exception;
}