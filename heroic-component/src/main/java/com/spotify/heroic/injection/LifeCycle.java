package com.spotify.heroic.injection;

import com.spotify.heroic.utils.Initializing;

import eu.toolchain.async.AsyncFuture;

public interface LifeCycle extends Initializing {
    public AsyncFuture<Void> start() throws Exception;

    public AsyncFuture<Void> stop() throws Exception;
}