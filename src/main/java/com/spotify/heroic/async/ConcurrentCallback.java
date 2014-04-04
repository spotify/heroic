package com.spotify.heroic.async;

import java.util.LinkedList;
import java.util.List;

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
public class ConcurrentCallback<T> implements Callback<T> {
    private final List<Handle<T>> handlers = new LinkedList<Handle<T>>();
    private final List<Cancelled> cancelled = new LinkedList<Cancelled>();
    private final List<Ended> ended = new LinkedList<Ended>();

    private State state = State.INITIALIZED;
    private Throwable error;
    private T result;

    @Override
    public synchronized void fail(Throwable error) {
        if (state != State.INITIALIZED)
            return;

        this.state = State.FAILED;
        this.error = error;

        for (Handle<T> handle : handlers) {
            invokeFailed(handle);
        }

        handlers.clear();
    }

    @Override
    public synchronized void finish(T result) {
        if (state != State.INITIALIZED)
            return;

        this.state = State.FINISHED;
        this.result = result;

        for (Handle<T> handle : handlers) {
            invokeFinished(handle);
        }

        handlers.clear();
    }

    @Override
    public synchronized void cancel() {
        if (state != State.INITIALIZED)
            return;

        this.state = ConcurrentCallback.State.CANCELLED;

        for (Cancelled cancel : cancelled) {
            invokeCancelled(cancel);
        }

        cancelled.clear();
    }

    @Override
    public void register(Handle<T> handle) {
        registerHandle(handle);
    }

    @Override
    public void register(Ended ended) {
        registerEnded(ended);
    }

    @Override
    public void register(Cancelled cancelled) {
        registerCancelled(cancelled);
    }

    private synchronized void registerHandle(Handle<T> handle) {
        switch (state) {
        case FINISHED:
            invokeFinished(handle);
            return;
        case CANCELLED:
            invokeCancelled(handle);
            return;
        case FAILED:
            invokeFailed(handle);
            return;
        default:
            break;
        }

        handlers.add(handle);
        cancelled.add(handle);
    }

    private synchronized void registerCancelled(Cancelled cancelled) {
        switch (state) {
        case CANCELLED:
            invokeCancelled(cancelled);
            return;
        default:
            break;
        }

        this.cancelled.add(cancelled);
    }

    private synchronized void registerEnded(Ended ended) {
        switch (state) {
        case FINISHED:
        case CANCELLED:
        case FAILED:
            invokeEnded(ended);
            return;
        default:
            break;
        }

        this.ended.add(ended);
    }

    private void invokeFinished(Handle<T> handle) {
        try {
            handle.finish(result);
        } catch (Exception e) {
            log.error("Failed to invoke finish callback", e);
        }
    }

    private void invokeFailed(Handle<T> handle) {
        try {
            handle.error(error);
        } catch (Exception e) {
            log.error("Failed to invoke error callback", e);
        }
    }

    private void invokeCancelled(Handle<T> handle) {
        try {
            handle.cancel();
        } catch (Exception e) {
            log.error("Failed to invoke cancel callback", e);
        }
    }

    private void invokeEnded(Ended ended) {
        try {
            ended.ended();
        } catch (Exception e) {
            log.error("Failed to invoke ended callback", e);
        }
    }

    private void invokeCancelled(Cancelled handle) {
        try {
            handle.cancel();
        } catch (Exception e) {
            log.error("Failed to invoke cancel callback", e);
        }
    }
}
