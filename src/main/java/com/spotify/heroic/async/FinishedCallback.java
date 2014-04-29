package com.spotify.heroic.async;

import java.util.List;

import lombok.extern.slf4j.Slf4j;

import com.codahale.metrics.Timer;

@Slf4j
public class FinishedCallback<T> implements Callback<T> {
    private final T value;

    public FinishedCallback(T value) {
        this.value = value;
    }

    /* all of these should do nothing. */
    @Override
    public Callback<T> fail(Throwable error) {
        return this;
    }

    @Override
    public Callback<T> finish(T result) {
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
    public <C> Callback<T> reduce(List<Callback<C>> callbacks, Timer timer,
            Callback.Reducer<C, T> reducer) {
        return this;
    }

    @Override
    public <C> Callback<T> reduce(List<Callback<C>> callbacks, Timer timer,
            Callback.StreamReducer<C, T> reducer) {
        return this;
    }

    /* all of these should be immediately resolved. */
    @Override
    public Callback<T> register(Callback.Handle<T> handle) {
        try {
            handle.finish(value);
        } catch (Exception e) {
            log.error("Failed to call handle finish callback", e);
        }

        return this;
    }

    @Override
    public Callback<T> register(Callback.Finishable finishable) {
        try {
            finishable.finish();
        } catch (Exception e) {
            log.error("Failed to call finish callback", e);
        }

        return this;
    }

    @Override
    public boolean isInitialized() {
        /* already done, so never initialized */
        return false;
    }
}
