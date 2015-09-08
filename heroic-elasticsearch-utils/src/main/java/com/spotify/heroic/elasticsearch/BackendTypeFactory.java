package com.spotify.heroic.elasticsearch;

public interface BackendTypeFactory<T, R> {
    BackendType<R> setup(T module);
}