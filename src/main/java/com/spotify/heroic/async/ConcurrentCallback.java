package com.spotify.heroic.async;

import java.util.Collection;
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

    private State state = State.INITIALIZED;

    private Exception error;
    private CancelReason cancelReason;
    private T result;

    @Override
    public Callback<T> fail(Exception error) {
        final Runnable runnable = synhronizedFail(error);

        if (runnable != null)
            runnable.run();

        return this;
    }

    @Override
    public Callback<T> resolve(T result) {
        final Runnable runnable = synchronizedFinish(result);

        if (runnable != null)
            runnable.run();

        return this;
    }

    @Override
    public Callback<T> cancel(CancelReason reason) {
        final Runnable runnable = synchronizedCancel(reason);

        if (runnable != null)
            runnable.run();

        return this;
    }

    @Override
    public Callback<T> register(Handle<T> handle) {
        registerHandle(handle);
        return this;
    }

    @Override
    public Callback<T> register(Finishable finishable) {
        registerFinishable(finishable);
        return this;
    }

    @Override
    public Callback<T> register(Cancellable cancellable) {
        registerCancellable(cancellable);
        return this;
    }

    @Override
    public synchronized boolean isInitialized() {
        return state == State.INITIALIZED;
    }

    /**
     * Make a point to clear all handles to assert that any associated memory
     * can be freed up ASAP.
     */
    private void clearAll() {
        handlers = null;
        cancellables = null;
        finishables = null;
    }

    private boolean registerHandle(Handle<T> handle) {
        final State s = addHandler(handle);

        switch (s) {
        case FINISHED:
            invokeFinished(handle);
            return true;
        case CANCELLED:
            invokeCancel(handle);
            return true;
        case FAILED:
            invokeFailed(handle);
            return true;
        default:
            return false;
        }
    }

    private boolean registerCancellable(Cancellable cancellable) {
        final State s = addCancellable(cancellable);

        switch (s) {
        case CANCELLED:
            invokeCancel(cancellable);
            return true;
        default:
            return false;
        }
    }

    private boolean registerFinishable(Finishable finishable) {
        final State s = addFinishable(finishable);

        switch (s) {
        case FINISHED:
        case CANCELLED:
        case FAILED:
            invokeFinish(finishable);
            return true;
        default:
            return false;
        }
    }

    private void invokeFinished(Handle<T> handle) {
        try {
            handle.resolved(result);
        } catch (final Exception e) {
            log.error("Failed to invoke finish callback", e);
        }
    }

    private void invokeFailed(Handle<T> handle) {
        try {
            handle.failed(error);
        } catch (final Exception e) {
            log.error("Failed to invoke error callback", e);
        }
    }

    private void invokeFinish(Finishable finishable) {
        try {
            finishable.finished();
        } catch (final Exception e) {
            log.error("Failed to invoke finish callback", e);
        }
    }

    private void invokeCancel(Cancellable cancellable) {
        try {
            cancellable.cancelled(cancelReason);
        } catch (final Exception e) {
            log.error("Failed to invoke cancel callback", e);
        }
    }

    @Override
    protected <C> Callback<C> newCallback() {
        return new ConcurrentCallback<C>();
    }

    /**
     * Synchronized functions.
     * 
     * rule a) The following fields must only be accessed in a synchronized
     * block.
     * 
     * state, handlers, cancelled, ended
     * 
     * rule b) No callback must be invoked in a synchronized block since that
     * will result in deadlocks.
     */

    private synchronized State addHandler(Handle<T> handle) {
        if (state == State.INITIALIZED) {
            handlers.add(handle);
            cancellables.add(handle);
        }

        return state;
    }

    private synchronized State addCancellable(Cancellable cancellable) {
        if (state == State.INITIALIZED) {
            cancellables.add(cancellable);
        }

        return state;
    }

    private synchronized State addFinishable(Finishable finishable) {
        if (state == State.INITIALIZED) {
            finishables.add(finishable);
        }

        return state;
    }

    private synchronized Runnable synhronizedFail(Exception error) {
        if (state != State.INITIALIZED)
            return null;

        this.state = State.FAILED;
        this.error = error;

        final Collection<Handle<T>> handlers = this.handlers;
        final Collection<Finishable> finishables = this.finishables;

        clearAll();

        // execution has to be deferred to avoid deadlocking on the same
        // synchronized block in case there are events calling the same
        // callback.
        return new Runnable() {
            @Override
            public void run() {
                for (final Handle<T> handle : handlers) {
                    invokeFailed(handle);
                }

                for (final Finishable finishable : finishables) {
                    invokeFinish(finishable);
                }
            };
        };
    }

    private synchronized Runnable synchronizedFinish(T result) {
        if (state != State.INITIALIZED)
            return null;

        this.state = State.FINISHED;
        this.result = result;

        final Collection<Handle<T>> handlers = this.handlers;
        final Collection<Finishable> finishables = this.finishables;

        clearAll();

        // execution has to be deferred to avoid deadlocking on the same
        // synchronized block in case there are events calling the same
        // callback.
        return new Runnable() {
            @Override
            public void run() {
                for (final Handle<T> handle : handlers) {
                    invokeFinished(handle);
                }

                for (final Finishable finishable : finishables) {
                    invokeFinish(finishable);
                }
            };
        };
    }

    private synchronized Runnable synchronizedCancel(CancelReason reason) {
        if (state != State.INITIALIZED)
            return null;

        this.state = State.CANCELLED;
        this.cancelReason = reason;

        final Collection<Cancellable> cancellables = this.cancellables;
        final Collection<Finishable> finishables = this.finishables;

        clearAll();

        // execution has to be deferred to avoid deadlocking on the same
        // synchronized block in case there are events calling the same
        // callback.
        return new Runnable() {
            @Override
            public void run() {
                for (final Cancellable cancellable : cancellables) {
                    invokeCancel(cancellable);
                }

                for (final Finishable finishable : finishables) {
                    invokeFinish(finishable);
                }
            };
        };
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