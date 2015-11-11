package com.spotify.heroic.async;

import eu.toolchain.async.AsyncFuture;

public interface AsyncObserver<T> {
    public AsyncFuture<Void> observe(final T value) throws Exception;

    public void cancel() throws Exception;

    public void fail(Throwable cause) throws Exception;

    public void end() throws Exception;
}