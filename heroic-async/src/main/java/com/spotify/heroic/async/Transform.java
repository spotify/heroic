package com.spotify.heroic.async;

public interface Transform<C, R> {
    R transform(C result) throws Exception;
}