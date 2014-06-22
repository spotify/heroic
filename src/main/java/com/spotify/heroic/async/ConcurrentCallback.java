package com.spotify.heroic.async;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;

import lombok.extern.slf4j.Slf4j;

/**
 * A class implementing the callback pattern concurrently in a way that any
 * thread can use the callback instance in a thread-safe manner.
 * 
 * The callback will retain it's result if it arrives early allowing for
 * graceful additions of late listeners. This allows for the deferred work to
 * start immediately.
 * 
 * It also allows for cancellation in any order.
 * 
 * <h1>Example</h1>
 * 
 * {@code Callback<Integer> callback = new ConcurrentCallback<Integer>();
 * 
 * new Thread(new Runnable() callback.finish(12); }).start();
 * 
 * callback.listen(new Callback.Handle<T>() { ... }); }
 * 
 * <h1>Synchronized functions.</h3>
 * 
 * rule a) The following fields must only be accessed in a block synchronized on
 * <code>state</code>: state, error, cancelReason, result, handlers,
 * cancellables, and finishables.
 * 
 * rule b) No callback must be invoked in a synchronized block since that will
 * result in deadlocks.
 * 
 * @author udoprog
 * 
 * @param <T>
 *            The type being deferred.
 */
@Slf4j
public class ConcurrentCallback<T> extends AbstractCallback<T> implements
Callback<T> {
    private List<Handle<T>> handlers = new LinkedList<Handle<T>>();
    private List<Cancellable> cancellables = new LinkedList<Cancellable>();
    private List<Finishable> finishables = new LinkedList<Finishable>();

    private volatile State state = State.READY;

    private Exception error;
    private CancelReason cancelReason;
    private T result;

    @Override
    public Callback<T> fail(Exception error) {
        if (state != State.READY)
            return this;

        synchronized (state) {
            if (state != State.READY)
                return this;

            this.state = State.FAILED;
            this.error = error;
        }

        for (final Handle<T> handle : handlers)
            callFailed(handle);

        for (final Finishable finishable : finishables)
            callFinished(finishable);

        clear();
        return this;
    }

    @Override
    public Callback<T> resolve(T result) {
        if (state != State.READY)
            return this;

        synchronized (state) {
            if (state != State.READY)
                return this;

            this.state = State.RESOLVED;
            this.result = result;
        }

        for (final Handle<T> handle : handlers)
            callResolved(handle);

        for (final Finishable finishable : finishables)
            callFinished(finishable);

        clear();

        return this;
    }

    @Override
    public Callback<T> cancel(CancelReason reason) {
        if (state != State.READY)
            return this;

        synchronized (state) {
            if (state != State.READY)
                return this;

            this.state = State.CANCELLED;
            this.cancelReason = reason;
        }

        for (final Handle<T> handle : handlers)
            callCancelled(handle);

        for (final Cancellable cancellable : cancellables)
            callCancelled(cancellable);

        for (final Finishable finishable : finishables)
            callFinished(finishable);

        clear();

        return this;
    }

    @Override
    public Callback<T> register(Handle<T> handle) {
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
    public Callback<T> register(ObjectHandle handle) {
        return register((Handle<T>) handle);
    }

    @Override
    public Callback<T> register(Finishable finishable) {
        if (addFinishable(finishable) != State.READY)
            callFinished(finishable);

        return this;
    }

    @Override
    public Callback<T> register(Cancellable cancellable) {
        if (addCancellable(cancellable) == State.CANCELLED)
            callCancelled(cancellable);

        return this;
    }

    @Override
    public boolean isReady() {
        return state == State.READY;
    }

    /**
     * Make a point to clear all handles to assert that any associated memory
     * can be freed up ASAP.
     */
    private void clear() {
        handlers = null;
        cancellables = null;
        finishables = null;
    }

    private void callResolved(Handle<T> handle) {
        try {
            handle.resolved(result);
        } catch (final Exception e) {
            log.error("Handle#resolve(T): failed", e);
        }
    }

    private void callFailed(Handle<T> handle) {
        try {
            handle.failed(error);
        } catch (final Exception e) {
            log.error("Handle#failed(Exception): failed", e);
        }
    }

    private void callFinished(Finishable finishable) {
        try {
            finishable.finished();
        } catch (final Exception e) {
            log.error("Finishable#finished(): failed", e);
        }
    }

    private void callCancelled(Cancellable cancellable) {
        try {
            cancellable.cancelled(cancelReason);
        } catch (final Exception e) {
            log.error("Cancellable#cancelled(CancelReason): failed", e);
        }
    }

    private void callCancelled(Handle<T> handle) {
        try {
            handle.cancelled(cancelReason);
        } catch (final Exception e) {
            log.error("Cancellable#cancelled(CancelReason): failed", e);
        }
    }

    @Override
    protected <C> Callback<C> newCallback() {
        return new ConcurrentCallback<C>();
    }

    private State addHandler(Handle<T> handle) {
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

    /**
     * Helper functions.
     */

    /**
     * Creates a new concurrent callback using the specified resolver.
     * 
     * @see {@link Callback#resolve(Executor, com.spotify.heroic.async.Callback.Resolver)}
     */
    public static <C> Callback<C> newResolve(Executor executor,
            final Resolver<C> resolver) {
        return new ConcurrentCallback<C>().resolve(executor, resolver);
    }

    /**
     * Creates a new concurrent callback using the specified reducer.
     * 
     * @see {@link Callback#reduce(List, com.spotify.heroic.async.Callback.Reducer)}
     */
    public static <C, T> Callback<T> newReduce(List<Callback<C>> queries,
            final Reducer<C, T> reducer) {
        return new ConcurrentCallback<T>().reduce(queries, reducer);
    }

    /**
     * Creates a new concurrent callback using the specified stream reducer.
     * 
     * @see {@link Callback#reduce(List, com.spotify.heroic.async.Callback.StreamReducer)}
     */
    public static <C, T> Callback<T> newReduce(List<Callback<C>> queries,
            final StreamReducer<C, T> reducer) {
        return new ConcurrentCallback<T>().reduce(queries, reducer);
    }
}