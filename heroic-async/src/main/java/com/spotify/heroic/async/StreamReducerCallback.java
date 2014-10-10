package com.spotify.heroic.async;

public interface StreamReducerCallback<T> {
    void finish(Future<T> callback, T result) throws Exception;

    void error(Future<T> callback, Exception error) throws Exception;

    void cancel(Future<T> callback, CancelReason reason) throws Exception;

    void done(int successful, int failed, int cancelled) throws Exception;
}