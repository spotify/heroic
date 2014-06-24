package com.spotify.heroic.async;

import java.util.List;
import java.util.concurrent.Executor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CancelledCallback<T> implements Callback<T> {
    private final CancelReason reason;

    public CancelledCallback(CancelReason reason) {
        this.reason = reason;
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

    /* all of these should be immediately cancelled. */
    @Override
    public Callback<T> register(Callback.Handle<T> handle) {
        try {
            handle.cancelled(reason);
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
        callback.cancel(reason);
        return this;
    }

    @Override
    public boolean isReady() {
        return false;
    }

    @Override
    public <C> Callback<C> transform(DeferredTransformer<T, C> transformer) {
        return new CancelledCallback<C>(reason);
    }

    @Override
    public <C> Callback<C> transform(Transformer<T, C> transformer) {
        return new CancelledCallback<C>(reason);
    }

    @Override
    public Callback<T> resolve(Executor executor, Resolver<T> resolver) {
        return this;
    }
}
