package com.spotify.heroic.async;

import java.util.ArrayList;
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
 * <code>
 * Callback<Integer> callback = new ConcurrentCallback<Integer>();
 * 
 * new Thread(new Runnable() {
 *     callback.finish(12);
 * }).start();
 * 
 * callback.listen(new Callback.Handle<T>() {
 *     ...
 * });
 * </code>
 * 
 * @author udoprog
 * 
 * @param <T>
 *            The type being deferred.
 */
@Slf4j
public class ConcurrentCallback<T> extends AbstractCallback<T> implements
        Callback<T> {
    private final List<Handle<T>> handlers = new LinkedList<Handle<T>>();
    private final List<Cancellable> cancellables = new LinkedList<Cancellable>();
    private final List<Finishable> finishables = new LinkedList<Finishable>();
    private State state = State.INITIALIZED;

    private Throwable error;
    private CancelReason cancelReason;
    private T result;

    @Override
    public Callback<T> fail(Throwable error) {
        final Runnable runnable = synhronizedFail(error);

        if (runnable != null)
            runnable.run();

        return this;
    }

    @Override
    public Callback<T> finish(T result) {
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
     * Make a point to clear all handles to make sure their memory can be freed
     * if necessary.
     */
    private void clearAll() {
        handlers.clear();
        cancellables.clear();
        finishables.clear();
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
            handle.finish(result);
        } catch (final Exception e) {
            log.error("Failed to invoke finish callback", e);
        }
    }

    private void invokeFailed(Handle<T> handle) {
        try {
            handle.error(error);
        } catch (final Exception e) {
            log.error("Failed to invoke error callback", e);
        }
    }

    private void invokeFinish(Finishable finishable) {
        try {
            finishable.finish();
        } catch (final Exception e) {
            log.error("Failed to invoke finish callback", e);
        }
    }

    private void invokeCancel(Cancellable cancellable) {
        try {
            cancellable.cancel(cancelReason);
        } catch (final Exception e) {
            log.error("Failed to invoke cancel callback", e);
        }
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

    private synchronized Runnable synhronizedFail(Throwable error) {
        if (state != State.INITIALIZED)
            return null;

        this.state = State.FAILED;
        this.error = error;

        final Collection<Handle<T>> handlers = new ArrayList<Handle<T>>(
                this.handlers);
        final Collection<Finishable> finishables = new ArrayList<Finishable>(
                this.finishables);

        clearAll();

        // defer the actual callbacking until we are out of the synchronized
        // block.
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

        final Collection<Handle<T>> handlers = new ArrayList<Handle<T>>(
                this.handlers);
        final Collection<Finishable> finishables = new ArrayList<Finishable>(
                this.finishables);

        clearAll();

        // defer the actual callbacking until we are out of the synchronized
        // block.
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

        final Collection<Handle<T>> cancellables = new ArrayList<Handle<T>>(
                this.handlers);
        final Collection<Finishable> finishables = new ArrayList<Finishable>(
                this.finishables);

        clearAll();

        return new Runnable() {
            @Override
            public void run() {
                for (final Handle<T> cancellable : cancellables) {
                    invokeCancel(cancellable);
                }

                for (final Finishable finishable : finishables) {
                    invokeFinish(finishable);
                }
            };
        };
    }

    public static <C> Callback<C> newResolve(Executor executor, final Resolver<C> resolver) {
        return new ConcurrentCallback<C>().resolve(executor, resolver);
    }

    public static <C, T> Callback<T> newReduce(List<Callback<C>> queries, final Reducer<C, T> reducer) {
        return new ConcurrentCallback<T>().reduce(queries, reducer);
    }

    public static <C, T> Callback<T> newReduce(List<Callback<C>> queries, final StreamReducer<C, T> reducer) {
        return new ConcurrentCallback<T>().reduce(queries, reducer);
    }

    @Override
    public <C> Callback<C> newCallback() {
        return new ConcurrentCallback<C>();
    }
}