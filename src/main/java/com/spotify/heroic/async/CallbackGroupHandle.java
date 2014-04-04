package com.spotify.heroic.async;

import java.util.Collection;

public abstract class CallbackGroupHandle<T, R> implements
        CallbackGroup.Handle<R> {
    private final Callback<T> callback;

    public CallbackGroupHandle(Callback<T> callback) {
        this.callback = callback;
    }

    @Override
    public void done(Collection<R> results, Collection<Throwable> errors,
            int cancelled) throws Exception {
        final T result;

        try {
            result = execute(results, errors, cancelled);
        } catch (final Throwable t) {
            callback.fail(t);
            return;
        }

        callback.finish(result);

    }

    public abstract T execute(Collection<R> results,
            Collection<Throwable> errors, int cancelled) throws Exception;
}
