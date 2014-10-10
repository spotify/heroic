package com.spotify.heroic.async;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * A callback which has already been resolved as 'cancelled'.
 *
 * @author udoprog
 *
 * @param <T>
 */
class CancelledFuture<T> implements Future<T> {
    private final CancelReason reason;

    public CancelledFuture(CancelReason reason) {
        this.reason = reason;
    }

    /* all of these should do nothing. */
    @Override
    public Future<T> fail(Exception error) {
        return this;
    }

    @Override
    public Future<T> resolve(T result) {
        return this;
    }

    @Override
    public Future<T> cancel(CancelReason reason) {
        return this;
    }

    @Override
    public Future<T> register(Cancellable cancellable) {
        return this;
    }

    @Override
    public <C> Future<T> reduce(List<Future<C>> callbacks, Reducer<C, T> reducer) {
        return this;
    }

    @Override
    public <C> Future<T> reduce(List<Future<C>> callbacks, StreamReducer<C, T> reducer) {
        return this;
    }

    /* all of these should be immediately cancelled. */
    @Override
    public Future<T> register(FutureHandle<T> handle) {
        try {
            handle.cancelled(reason);
        } catch (final Exception e) {
            throw new RuntimeException("Failed to call handle finish callback", e);
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
        try {
            finishable.finished();
        } catch (final Exception e) {
            throw new RuntimeException("Failed to call finish callback", e);
        }

        return this;
    }

    @Override
    public Future<T> register(Future<T> callback) {
        callback.cancel(reason);
        return this;
    }

    @Override
    public boolean isReady() {
        return false;
    }

    @Override
    public <C> Future<C> transform(DeferredTransformer<T, C> transformer) {
        return new CancelledFuture<C>(reason);
    }

    @Override
    public <C> Future<C> transform(Transformer<T, C> transformer) {
        return new CancelledFuture<C>(reason);
    }

    @Override
    public <C> Future<C> transform(Transformer<T, C> transformer, ErrorTransformer<C> error) {
        return new CancelledFuture<C>(reason);
    }

    @Override
    public Future<T> resolve(Executor executor, Resolver<T> resolver) {
        return this;
    }

    @Override
    public T get() throws InterruptedException, CancelledException, FailedException {
        throw new CancelledException(reason);
    }
}
