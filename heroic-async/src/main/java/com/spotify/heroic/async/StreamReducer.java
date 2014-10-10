package com.spotify.heroic.async;

public interface StreamReducer<C, R> {
    /**
     * Implement to trigger on one resolved.
     */
    void resolved(Future<C> callback, C result) throws Exception;

    /**
     * Implement to trigger on one failed.
     */
    void failed(Future<C> callback, Exception error) throws Exception;

    /**
     * Implement to trigger on one cancelled.
     */
    void cancelled(Future<C> callback, CancelReason reason) throws Exception;

    /**
     * Implement to fire when all callbacks have been resolved.
     */
    R resolved(int successful, int failed, int cancelled) throws Exception;
}