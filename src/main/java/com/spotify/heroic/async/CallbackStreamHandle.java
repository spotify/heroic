package com.spotify.heroic.async;

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
public abstract class CallbackStreamHandle<T, R> implements
        CallbackStream.Handle<R> {
    private final Callback<T> callback;
    private final Timer timer;

    public CallbackStreamHandle(Callback<T> callback, Timer timer) {
        this.callback = callback;
        this.timer = timer;
    }

    @Override
    public void done(int successful, int failed, int cancelled) {
        if (!callback.isInitialized())
            return;

        final T result;
        final Timer.Context context = timer.time();

        try {
            result = execute(successful, failed, cancelled);
        } catch (final Throwable t) {
            callback.fail(t);
            return;
        } finally {
            context.stop();
        }

        callback.finish(result);
    }

    public abstract T execute(int successful, int failed, int cancelled)
            throws Exception;
}
