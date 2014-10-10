package com.spotify.heroic.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of {@link Future#reduce(List, Future.StreamReducer)}.
 *
 * @author udoprog
 *
 * @param <T>
 */
class StreamReducerImplementation<T> implements Cancellable {
    private final AtomicInteger countdown;
    private final List<Future<T>> callbacks;
    private final AtomicInteger successful = new AtomicInteger();
    private final AtomicInteger failed = new AtomicInteger();
    private final AtomicInteger cancelled = new AtomicInteger();

    public StreamReducerImplementation(Collection<Future<T>> callbacks, final StreamReducerCallback<T> handle) {
        this.countdown = new AtomicInteger(callbacks.size());
        this.callbacks = new ArrayList<Future<T>>(callbacks);

        for (final Future<T> callback : callbacks) {
            callback.register(new FutureHandle<T>() {
                @Override
                public void failed(Exception e) throws Exception {
                    failed.incrementAndGet();
                    handleError(handle, callback, e);
                    StreamReducerImplementation.this.check(handle);
                }

                @Override
                public void resolved(T result) throws Exception {
                    successful.incrementAndGet();
                    handleFinish(handle, callback, result);
                    StreamReducerImplementation.this.check(handle);
                }

                @Override
                public void cancelled(CancelReason reason) throws Exception {
                    cancelled.incrementAndGet();
                    handleCancel(handle, callback, reason);
                    StreamReducerImplementation.this.check(handle);
                }
            });
        }

        if (callbacks.isEmpty())
            handleDone(handle);
    }

    private void handleError(StreamReducerCallback<T> handle, Future<T> callback, Exception error) {
        try {
            handle.error(callback, error);
        } catch (final Exception t) {
            throw new RuntimeException("Failed to call error on handle", t);
        }
    }

    private void handleFinish(StreamReducerCallback<T> handle, Future<T> callback, T result) {
        try {
            handle.finish(callback, result);
        } catch (final Exception t) {
            throw new RuntimeException("Failed to call finish on handle", t);
        }
    }

    private void handleCancel(StreamReducerCallback<T> handle, Future<T> callback, CancelReason reason) {
        try {
            handle.cancel(callback, reason);
        } catch (final Exception t) {
            throw new RuntimeException("Failed to call cancel on handle", t);
        }
    }

    private void handleDone(StreamReducerCallback<T> handle) {
        try {
            handle.done(successful.get(), failed.get(), cancelled.get());
        } catch (final Throwable t) {
            throw new RuntimeException("Failed to call done on handle", t);
        }
    }

    private void check(StreamReducerCallback<T> handle) throws Exception {
        final int value = countdown.decrementAndGet();

        if (value != 0)
            return;

        handleDone(handle);
    }

    /* cancel all queries in this group */
    @Override
    public void cancelled(CancelReason reason) {
        for (final Future<T> callback : callbacks) {
            callback.cancel(reason);
        }
    }
}
