package com.spotify.heroic.httpclient;

import eu.toolchain.async.AsyncFuture;

public interface HttpClientSession {
    public <R, T> AsyncFuture<T> post(R request, Class<T> clazz, String endpoint);

    public <T> AsyncFuture<T> get(Class<T> clazz, String endpoint);
}
