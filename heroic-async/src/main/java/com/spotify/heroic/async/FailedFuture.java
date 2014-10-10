package com.spotify.heroic.async;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * A callback which has already been resolved as 'failed'.
 *
 * @author udoprog
 *
 * @param <T>
 */
class FailedFuture<T> implements Future<T> {
    private final Exception error;

    public FailedFuture(Exception error) {
        this.error = error;
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

    /* all of these should be immediately failed/finished. */
    @Override
    public Future<T> register(FutureHandle<T> handle) {
        try {
            handle.failed(error);
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
        callback.fail(error);
        return this;
    }

    @Override
    public boolean isReady() {
        /* already done, so never initialized */
        return false;
    }

    @Override
    public <C> Future<C> transform(DeferredTransformer<T, C> transformer) {
        return new FailedFuture<C>(error);
    }

    @Override
    public <C> Future<C> transform(Transformer<T, C> transformer) {
        return new FailedFuture<C>(error);
    }

    @Override
    public <C> Future<C> transform(Transformer<T, C> transformer, ErrorTransformer<C> error) {
        if (error == null) {
            return new FailedFuture<C>(this.error);
        }

        try {
            return new ResolvedFuture<>(error.transform(this.error));
        } catch (final Exception e2) {
            return new FailedFuture<C>(e2);
        }
    }

    @Override
    public Future<T> resolve(Executor executor, Resolver<T> resolver) {
        return this;
    }

    @Override
    public T get() throws FailedException {
        throw new FailedException(error);
    }
}
