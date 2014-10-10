package com.spotify.heroic.async;

public interface ErrorTransformer<R> {
    R transform(Exception e) throws Exception;
}