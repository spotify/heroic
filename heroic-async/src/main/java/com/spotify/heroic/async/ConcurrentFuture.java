package com.spotify.heroic.async;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * A class implementing the callback pattern concurrently in a way that any thread can use the callback instance in a
 * thread-safe manner.
 * 
 * The callback will retain it's result if it arrives early allowing for graceful additions of late listeners. This
 * allows for the deferred work to start immediately.
 * 
 * It also allows for cancellation in any order.
 * 
 * <h1>Example</h1>
 * 
 * <pre>
 * {@code
 *     Callback<Integer> callback = new ConcurrentCallback<Integer>();
 *     new Thread(new Runnable() callback.resolve(12); }).start();
 *     callback.listen(new Callback.Handle<T>() { ... });
 * }
 * </pre>
 * 
 * <h1>Synchronized functions.</h3>
 * 
 * rule a) The following fields must only be accessed in a block synchronized on <code>state</code>: state, error,
 * cancelReason, result, handlers, cancellables, and finishables.
 * 
 * rule b) No other callback must be invoked in a synchronized block since that could result in deadlocks because _this_
 * callback could be part of another callbacks chain.
 * 
 * @author udoprog
 * 
 * @param <T>
 *            The type being deferred.
 */
class ConcurrentFuture<T> extends AbstractFuture<T> implements Future<T> {
    private List<FutureHandle<T>> handlers = new LinkedList<FutureHandle<T>>();
    private List<Cancellable> cancellables = new LinkedList<Cancellable>();
    private List<Finishable> finishables = new LinkedList<Finishable>();

    private volatile State state = State.READY;

    private Exception error;
    private CancelReason cancelReason;
    private T result;

    @Override
    public Future<T> fail(Exception error) {
        if (state != State.READY)
            return this;

        synchronized (state) {
            if (state != State.READY)
                return this;

            this.state = State.FAILED;
            this.error = error;
        }

        for (final FutureHandle<T> handle : handlers)
            callFailed(handle);

        for (final Finishable finishable : finishables)
            callFinished(finishable);

        clear();
        return this;
    }

    @Override
    public Future<T> resolve(T result) {
        if (state != State.READY)
            return this;

        synchronized (state) {
            if (state != State.READY)
                return this;

            this.state = State.RESOLVED;
            this.result = result;
        }

        for (final FutureHandle<T> handle : handlers)
            callResolved(handle);

        for (final Finishable finishable : finishables)
            callFinished(finishable);

        clear();

        return this;
    }

    @Override
    public Future<T> cancel(CancelReason reason) {
        if (state != State.READY)
            return this;

        synchronized (state) {
            if (state != State.READY)
                return this;

            this.state = State.CANCELLED;
            this.cancelReason = reason;
        }

        for (final FutureHandle<T> handle : handlers)
            callCancelled(handle);

        for (final Cancellable cancellable : cancellables)
            callCancelled(cancellable);

        for (final Finishable finishable : finishables)
            callFinished(finishable);

        clear();

        return this;
    }

    @Override
    public Future<T> register(FutureHandle<T> handle) {
        switch (addHandler(handle)) {
        case RESOLVED:
            callResolved(handle);
            break;
        case CANCELLED:
            callCancelled(handle);
            break;
        case FAILED:
            callFailed(handle);
            break;
        default:
            break;
        }

        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Future<T> register(ObjectHandle handle) {
        return register((FutureHandle<T>) handle);
    }

    @Override
    public Future<T> register(Finishable finishable) {
        if (addFinishable(finishable) != State.READY)
            callFinished(finishable);

        return this;
    }

    @Override
    public Future<T> register(Cancellable cancellable) {
        if (addCancellable(cancellable) == State.CANCELLED)
            callCancelled(cancellable);

        return this;
    }

    @Override
    public boolean isReady() {
        return state == State.READY;
    }

    /**
     * Make a point to clear all handles to assert that any associated memory can be freed up ASAP.
     */
    private void clear() {
        handlers = null;
        cancellables = null;
        finishables = null;
    }

    private void callResolved(FutureHandle<T> handle) {
        try {
            handle.resolved(result);
        } catch (final Exception e) {
            throw new RuntimeException("Handle#resolved(T): failed", e);
        }
    }

    private void callFailed(FutureHandle<T> handle) {
        try {
            handle.failed(error);
        } catch (final Exception e) {
            throw new RuntimeException("Handle#failed(Exception): failed", e);
        }
    }

    private void callFinished(Finishable finishable) {
        try {
            finishable.finished();
        } catch (final Exception e) {
            throw new RuntimeException("Finishable#finished(): failed", e);
        }
    }

    private void callCancelled(Cancellable cancellable) {
        try {
            cancellable.cancelled(cancelReason);
        } catch (final Exception e) {
            throw new RuntimeException("Cancellable#cancelled(CancelReason): failed", e);
        }
    }

    private void callCancelled(FutureHandle<T> handle) {
        try {
            handle.cancelled(cancelReason);
        } catch (final Exception e) {
            throw new RuntimeException("Cancellable#cancelled(CancelReason): failed", e);
        }
    }

    @Override
    protected <C> Future<C> newCallback() {
        return new ConcurrentFuture<C>();
    }

    private State addHandler(FutureHandle<T> handle) {
        synchronized (state) {
            if (state != State.READY)
                return state;

            handlers.add(handle);
        }

        return State.READY;
    }

    private State addCancellable(Cancellable cancellable) {
        synchronized (state) {
            if (state != State.READY)
                return state;

            cancellables.add(cancellable);
        }

        return State.READY;
    }

    private State addFinishable(Finishable finishable) {
        synchronized (state) {
            if (state != State.READY)
                return state;

            finishables.add(finishable);
        }

        return State.READY;
    }

    @Override
    public T get() throws InterruptedException, CancelledException, FailedException {
        final CountDownLatch latch = new CountDownLatch(1);

        register(new Finishable() {
            @Override
            public void finished() throws Exception {
                latch.countDown();
            }
        });

        latch.await();

        switch (state) {
        case CANCELLED:
            throw new CancelledException(cancelReason);
        case FAILED:
            throw new FailedException(error);
        case RESOLVED:
            return result;
        default:
            throw new IllegalStateException(state.toString());
        }
    }
}