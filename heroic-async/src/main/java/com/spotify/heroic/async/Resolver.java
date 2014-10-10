package com.spotify.heroic.async;

public interface Resolver<R> {
    R resolve() throws Exception;
}