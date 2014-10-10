package com.spotify.heroic.async;

public interface FutureHandle<T> {
    void cancelled(CancelReason reason) throws Exception;

    void failed(Exception e) throws Exception;

    void resolved(T result) throws Exception;
}