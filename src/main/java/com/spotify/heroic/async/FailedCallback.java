package com.spotify.heroic.async;

import java.util.List;
import java.util.concurrent.Executor;

import lombok.extern.slf4j.Slf4j;

/**
 * A callback which has already been resolved as 'failed'.
 *
 * @author udoprog
 *
 * @param <T>
 */
@Slf4j
public class FailedCallback<T> implements Callback<T> {
    private final Exception error;

    public FailedCallback(Exception error) {
        this.error = error;
    }

    /* all of these should do nothing. */
    @Override
    public Callback<T> fail(Exception error) {
        return this;
    }

    @Override
    public Callback<T> resolve(T result) {
        return this;
    }

    @Override
    public Callback<T> cancel(CancelReason reason) {
        return this;
    }

    @Override
    public Callback<T> register(Callback.Cancellable cancellable) {
        return this;
    }

    @Override
    public <C> Callback<T> reduce(List<Callback<C>> callbacks,
            Callback.Reducer<C, T> reducer) {
        return this;
    }

    @Override
    public <C> Callback<T> reduce(List<Callback<C>> callbacks,
            StreamReducer<C, T> reducer) {
        return this;
    }

    /* all of these should be immediately failed/finished. */
    @Override
    public Callback<T> register(Callback.Handle<T> handle) {
        try {
            handle.failed(error);
        } catch (Exception e) {
            log.error("Failed to call handle finish callback", e);
        }

        return this;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Callback<T> register(ObjectHandle handle) {
        return register((Handle<T>) handle);
    }

    @Override
    public Callback<T> register(Callback.Finishable finishable) {
        try {
            finishable.finished();
        } catch (Exception e) {
            log.error("Failed to call finish callback", e);
        }

        return this;
    }

    @Override
    public Callback<T> register(Callback<T> callback) {
        callback.fail(error);
        return this;
    }

    @Override
    public boolean isReady() {
        /* already done, so never initialized */
        return false;
    }

    @Override
    public <C> Callback<C> transform(DeferredTransformer<T, C> transformer) {
        return new FailedCallback<C>(error);
    }

    @Override
    public <C> Callback<C> transform(Transformer<T, C> transformer) {
        return new FailedCallback<C>(error);
    }

    @Override
    public Callback<T> resolve(Executor executor, Resolver<T> resolver) {
        return this;
    }

    @Override
    public T get() throws FailedException {
        throw new FailedException(error);
    }
}
