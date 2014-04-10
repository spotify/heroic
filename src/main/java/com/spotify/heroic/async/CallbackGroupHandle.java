package com.spotify.heroic.async;

import java.util.Collection;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;

/**
 * A helper class that will act as a CallbackGroup.Handle reporting it's result
 * to a Callback.Handle.
 * 
 * @author udoprog
 * 
 * @param <T>
 * @param <R>
 */
@Slf4j
public abstract class CallbackGroupHandle<T, R> implements
        CallbackGroup.Handle<R> {
    private final Callback<T> callback;
    private final Timer timer;

    public CallbackGroupHandle(Callback<T> callback, Timer timer) {
        this.callback = callback;
        this.timer = timer;
    }

    @Override
    public void done(Collection<R> results, Collection<Throwable> errors,
            Collection<CancelReason> cancelled) {
        if (!callback.isInitialized()) {
            return;
        }

        final T result;
        final Timer.Context context = timer.time();

        try {
            result = execute(results, errors, cancelled);
        } catch (final Throwable t) {
            callback.fail(t);
            return;
        } finally {
            context.stop();
        }

        callback.finish(result);
    }

    public abstract T execute(Collection<R> results,
            Collection<Throwable> errors, Collection<CancelReason> cancelled)
            throws Exception;
}
