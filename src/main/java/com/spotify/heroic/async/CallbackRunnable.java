package com.spotify.heroic.async;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;

/**
 * Helper class that allows for safer Runnable implementations meant to wrap
 * Query<T>
 * 
 * @author udoprog
 * @param <T>
 *            The type expected to be returned by the implemented execute
 *            function and realized for the specified query.
 */
@Slf4j
public abstract class CallbackRunnable<T> implements Runnable {
    private final Callback<T> callback;
    private final Timer timer;

    public CallbackRunnable(Callback<T> callback, Timer timer) {
        this.callback = callback;
        this.timer = timer;
    }

    @Override
    public void run() {
        if (!callback.isInitialized())
            return;

        final T result;
        final Timer.Context context = timer.time();

        try {
            result = execute();
        } catch (final Throwable t) {
            callback.fail(t);
            return;
        } finally {
            context.stop();
        }

        callback.finish(result);
    }

    public abstract T execute() throws Exception;
}
