package com.spotify.heroic.async;

import java.util.concurrent.TimeUnit;

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
    private final String task;
    private final Callback<T> callback;
    private final Timer timer;

    public CallbackRunnable(String task, Timer timer, Callback<T> callback) {
        this.task = task;
        this.timer = timer;
        this.callback = callback;
    }

    @Override
    public void run() {
        if (!callback.isInitialized())
            return;

        final T result;
        final Timer.Context context = timer.time();
        log.debug("{} (id {})", task, hashCode());

        try {
            result = execute();
        } catch (final Throwable t) {
            callback.fail(t);
            return;
        } finally {
            final long time = context.stop();
            final long ms = TimeUnit.MILLISECONDS.convert(time,
                    TimeUnit.NANOSECONDS);
            // log.debug("{} (id {}, took {}ms)", task, hashCode(), ms);
        }

        callback.finish(result);
    }

    public abstract T execute() throws Exception;
}
