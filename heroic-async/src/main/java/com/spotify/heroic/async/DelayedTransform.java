package com.spotify.heroic.async;

public interface DelayedTransform<C, R> {
    Future<R> transform(C result) throws Exception;
}