package com.spotify.heroic.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of {@link Future#reduce(List, Reducer)}.
 *
 * @author udoprog
 *
 * @param <T>
 */
class FutureReducer<T> implements Cancellable {
    public static interface Handle<T> {
        void done(Collection<T> results, Collection<Exception> errors, Collection<CancelReason> cancelled)
                throws Exception;
    }

    private final AtomicInteger countdown;
    private final List<Future<T>> callbacks;
    private final Handle<T> handle;
    private volatile boolean done = false;

    private final Queue<Exception> errors = new ConcurrentLinkedQueue<Exception>();
    private final Queue<T> results = new ConcurrentLinkedQueue<T>();
    private final Queue<CancelReason> cancelled = new ConcurrentLinkedQueue<CancelReason>();

    private final FutureHandle<T> listener = new FutureHandle<T>() {
        @Override
        public void failed(Exception e) throws Exception {
            errors.add(e);
            FutureReducer.this.checkIn();
        }

        @Override
        public void resolved(T result) throws Exception {
            if (result == null)
                throw new NullPointerException(
                        "CallbackGroup cannot handle null results (due to using a Queue for storing results)");

            results.add(result);
            FutureReducer.this.checkIn();
        }

        @Override
        public void cancelled(CancelReason reason) throws Exception {
            cancelled.add(reason);
            FutureReducer.this.checkIn();
        }
    };

    public FutureReducer(Collection<Future<T>> callbacks, Handle<T> handle) {
        this.countdown = new AtomicInteger(callbacks.size());
        this.callbacks = new ArrayList<Future<T>>(callbacks);
        this.handle = handle;
        this.done = false;

        for (final Future<T> callback : callbacks)
            callback.register(listener);

        if (this.callbacks.isEmpty())
            end();
    }

    /**
     * Checks in a call back. It also wraps up the group if all the callbacks have checked in.
     */
    private void checkIn() {
        final int value = countdown.decrementAndGet();

        if (value != 0)
            return;

        end();
    }

    private synchronized void end() {
        if (done)
            return;

        try {
            handle.done(results, errors, cancelled);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to call Handle#done", e);
        }

        done = true;
    }

    /* cancel all queries in this group */
    @Override
    public void cancelled(CancelReason reason) {
        for (final Future<T> callback : callbacks) {
            callback.cancel(reason);
        }
    }
}
