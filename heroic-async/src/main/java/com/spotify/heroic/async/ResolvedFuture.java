package com.spotify.heroic.async;

import java.util.List;
import java.util.concurrent.Executor;

/**
 * A callback which has already been resolved as 'resolved'.
 *
 * @author udoprog
 *
 * @param <T>
 */
public class ResolvedFuture<T> implements Future<T> {
    private final T value;

    public ResolvedFuture(T value) {
        this.value = value;
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

    /* all of these should be immediately resolved. */
    @Override
    public Future<T> register(FutureHandle<T> handle) {
        try {
            handle.resolved(value);
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
        callback.resolve(value);
        return this;
    }

    @Override
    public boolean isReady() {
        /* already done, so never initialized */
        return false;
    }

    @Override
    public <C> Future<C> transform(DeferredTransformer<T, C> transformer) {
        try {
            return transformer.transform(value);
        } catch (final Exception e) {
            return new FailedFuture<C>(e);
        }
    }

    @Override
    public <C> Future<C> transform(Transformer<T, C> transformer) {
        return transform(transformer, null);
    }

    @Override
    public <C> Future<C> transform(Transformer<T, C> transformer, ErrorTransformer<C> error) {
        try {
            return new ResolvedFuture<C>(transformer.transform(value));
        } catch (final Exception e) {
            try {
                return new ResolvedFuture<>(error.transform(e));
            } catch (final Exception e2) {
                return new FailedFuture<C>(e2);
            }
        }
    }

    @Override
    public Future<T> resolve(Executor executor, Resolver<T> resolver) {
        return this;
    }

    @Override
    public T get() {
        return value;
    }
}
