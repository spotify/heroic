package com.spotify.heroic.async;

public interface DeferredTransformer<C, R> {
    Future<R> transform(C result) throws Exception;
}