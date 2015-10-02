package com.spotify.heroic.metric;

import java.util.List;

import com.google.common.collect.ImmutableList;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public abstract class AbstractMetricBackend implements MetricBackend {
    private final AsyncFramework async;

    @Override
    public AsyncFuture<List<String>> serializeKeyToHex(BackendKey key) {
        return async.resolved(ImmutableList.of());
    }

    @Override
    public AsyncFuture<List<BackendKey>> deserializeKeyFromHex(String key) {
        return async.resolved(ImmutableList.of());
    }

    @Override
    public AsyncFuture<BackendKeySet> keys(BackendKey start, int limit, QueryOptions options) {
        return async.resolved(new BackendKeySet());
    }
}