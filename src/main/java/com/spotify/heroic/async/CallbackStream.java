package com.spotify.heroic.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CallbackStream<T> implements Callback.Cancellable {
    public static interface Handle<T> {
        void finish(CallbackStream<T> stream, Callback<T> callback, T result)
                throws Exception;

        void error(CallbackStream<T> stream, Callback<T> callback,
        		Exception error) throws Exception;

        void cancel(CallbackStream<T> stream, Callback<T> callback,
                CancelReason reason) throws Exception;

        void done(int successful, int failed, int cancelled) throws Exception;
    }

    private final AtomicInteger countdown;
    private final List<Callback<T>> callbacks;
    private final AtomicInteger successful = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private final AtomicInteger cancelled = new AtomicInteger();

    public CallbackStream(Collection<Callback<T>> callbacks,
            final Handle<T> handle) {
        this.countdown = new AtomicInteger(callbacks.size());
        this.callbacks = new ArrayList<Callback<T>>(callbacks);

        for (final Callback<T> callback : callbacks) {
            callback.register(new Callback.Handle<T>() {
                @Override
                public void error(Exception e) throws Exception {
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
                public void cancel(CancelReason reason) throws Exception {
                    cancelled.incrementAndGet();
                    handleCancel(handle, callback, reason);
                    CallbackStream.this.check(handle);
                }
            });
        }

        if (callbacks.isEmpty())
            handleDone(handle);
    }

    private void handleError(Handle<T> handle, Callback<T> callback,
    		Exception error) {
        try {
            handle.error(this, callback, error);
        } catch (final Exception t) {
            log.error("Failed to call error on handle", t);
        }
    }

    private void handleFinish(Handle<T> handle, Callback<T> callback, T result) {
        try {
            handle.finish(this, callback, result);
        } catch (final Exception t) {
            log.error("Failed to call finish on handle", t);
        }
    }

    private void handleCancel(Handle<T> handle, Callback<T> callback,
            CancelReason reason) {
        try {
            handle.cancel(this, callback, reason);
        } catch (final Exception t) {
            log.error("Failed to call cancel on handle", t);
        }
    }

    private void handleDone(Handle<T> handle) {
        try {
            handle.done(successful.get(), failed.get(), cancelled.get());
        } catch (final Throwable t) {
            log.error("Failed to call done on handle", t);
        }
    }

    private void check(Handle<T> handle) throws Exception {
        final int value = countdown.decrementAndGet();

        if (value != 0)
            return;

        handleDone(handle);
    }

    /* cancel all queries in this group */
    @Override
    public void cancel(CancelReason reason) {
        log.warn("Cancelling all callbacks");

        for (final Callback<T> callback : callbacks) {
            callback.cancel(reason);
        }
    }
}
