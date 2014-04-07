package com.spotify.heroic.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CallbackStream<T> {
    public static interface Handle<T> {
        void finish(Callback<T> callback, T result) throws Exception;

        void error(Callback<T> callback, Throwable error) throws Exception;

        void cancel(Callback<T> callback) throws Exception;

        void done(int successful, int failed, int cancelled) throws Exception;
    }

    private final AtomicInteger countdown;
    private final List<Callback<T>> callbacks;
    private final AtomicInteger successful = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private final AtomicInteger cancelled = new AtomicInteger();

    public CallbackStream(Collection<Callback<T>> callbacks,
            final Handle<T> handle) throws Exception {
        this.countdown = new AtomicInteger(callbacks.size());
        this.callbacks = new ArrayList<Callback<T>>(callbacks);

        for (final Callback<T> callback : callbacks) {
            callback.register(new Callback.Handle<T>() {
                @Override
                public void error(Throwable e) throws Exception {
                    failed.incrementAndGet();
                    handleError(handle, callback, e);
                    CallbackStream.this.check(handle);
                }

                @Override
                public void finish(T result) throws Exception {
                    successful.incrementAndGet();
                    handleFinish(handle, callback, result);
                    CallbackStream.this.check(handle);
                }

                @Override
                public void cancel() throws Exception {
                    cancelled.incrementAndGet();
                    handleCancel(handle, callback);
                    CallbackStream.this.check(handle);
                }
            });
        }

        if (callbacks.isEmpty())
            handleDone(handle);
    }

    /* cancel all queries in this group */
    public void cancel() {
        log.warn("Cancelling");

        for (Callback<T> callback : callbacks) {
            callback.cancel();
        }
    }

    private void handleError(Handle<T> handle, Callback<T> callback,
            Throwable error) {
        try {
            handle.error(callback, error);
        } catch (Throwable t) {
            log.error("Failed to call error on handle", t);
        }
    }

    private void handleFinish(Handle<T> handle, Callback<T> callback, T result) {
        try {
            handle.finish(callback, result);
        } catch (Throwable t) {
            log.error("Failed to call finish on handle", t);
        }
    }

    private void handleCancel(Handle<T> handle, Callback<T> callback) {
        try {
            handle.cancel(callback);
        } catch (Throwable t) {
            log.error("Failed to call cancel on handle", t);
        }
    }

    private void handleDone(Handle<T> handle) {
        try {
            handle.done(successful.get(), failed.get(), cancelled.get());
        } catch (Throwable t) {
            log.error("Failed to call done on handle", t);
        }
    }

    private void check(Handle<T> handle) throws Exception {
        int value = countdown.decrementAndGet();

        if (value != 0)
            return;

        handleDone(handle);
    }
}
