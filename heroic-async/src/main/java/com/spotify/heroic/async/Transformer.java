package com.spotify.heroic.async;

public interface Transformer<C, R> {
    R transform(C result) throws Exception;
}