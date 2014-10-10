package com.spotify.heroic.async;

/**
 * Convenience class that can be used to extend and implement only a subset of the functionality of
 * {@link StreamReducer}.
 *
 * Note: {@link StreamReducer#resolved(int, int, int)} is the minimal required implementation since it is not
 * provided here.
 *
 * @author udoprog
 */
public abstract class DefaultStreamReducer<C, R> implements StreamReducer<C, R> {
    /**
     * Override to trigger on one resolved.
     */
    @Override
    public void resolved(Future<C> callback, C result) throws Exception {
    }

    /**
     * Override to trigger on one failed.
     */
    @Override
    public void failed(Future<C> callback, Exception error) throws Exception {
    }

    /**
     * Override to trigger on one cancelled.
     */
    @Override
    public void cancelled(Future<C> callback, CancelReason reason) throws Exception {
    }
}