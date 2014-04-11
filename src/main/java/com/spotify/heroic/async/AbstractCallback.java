package com.spotify.heroic.async;

import java.util.Collection;
import java.util.List;

import com.codahale.metrics.Timer;

/**
 * Provide some simple common implementations of a callback.
 * 
 * @author udoprog
 * 
 * @param <T>
 *            The value type of the callback.
 */
public abstract class AbstractCallback<T> implements Callback<T> {
    @Override
    public <C> Callback<T> reduce(List<Callback<C>> queries, Timer timer,
            final Reducer<C, T> reducer) {

        final CallbackGroupHandle<T, C> handle = new CallbackGroupHandle<T, C>(
                this, timer) {
            @Override
            public T execute(Collection<C> results,
                    Collection<Throwable> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                return reducer.done(results, errors, cancelled);
            }

        };

        return register(new CallbackGroup<C>(queries, handle));
    }

    @Override
    public <C> Callback<T> reduce(List<Callback<C>> queries, final Timer timer,
            final StreamReducer<C, T> reducer) {

        final CallbackStreamHandle<T, C> handle = new CallbackStreamHandle<T, C>(
                this, timer) {
            @Override
            public void finish(CallbackStream<C> stream, Callback<C> callback,
                    C result) throws Exception {
                reducer.finish(stream, callback, result);
            }

            @Override
            public void error(CallbackStream<C> stream, Callback<C> callback,
                    Throwable error) throws Exception {
                reducer.error(stream, callback, error);
            }

            @Override
            public void cancel(CallbackStream<C> stream, Callback<C> callback,
                    CancelReason reason) throws Exception {
                reducer.cancel(stream, callback, reason);
            }

            @Override
            public T execute(int successful, int failed, int cancelled)
                    throws Exception {
                return reducer.done(successful, failed, cancelled);
            }
        };

        return register(new CallbackStream<C>(queries, handle));
    }
}
