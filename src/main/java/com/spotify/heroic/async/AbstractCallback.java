package com.spotify.heroic.async;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

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
    public <C> Callback<T> reduce(List<Callback<C>> queries, final Reducer<C, T> reducer) {
        final CallbackGroup.Handle<C> handle = new CallbackGroup.Handle<C>() {
            @Override
            public void done(Collection<C> results,
                    Collection<Throwable> errors,
                    Collection<CancelReason> cancelled) throws Exception {
                if (!AbstractCallback.this.isInitialized())
                    return;

                try {
                    AbstractCallback.this.finish(reducer.done(results, errors, cancelled));
                } catch(Throwable error) {
                    AbstractCallback.this.fail(error);
                }
            }
        };

        return register(new CallbackGroup<C>(queries, handle));
    }

    @Override
    public <C> Callback<T> reduce(List<Callback<C>> queries, final StreamReducer<C, T> reducer) {
        final CallbackStream.Handle<C> handle = new CallbackStream.Handle<C>() {
            @Override
            public void finish(CallbackStream<C> stream, Callback<C> callback,
                    C result) throws Exception {
                reducer.finish(stream, callback, result);
            }

            @Override
            public void error(CallbackStream<C> stream, Callback<C> callback, Throwable error) throws Exception {
                reducer.error(stream, callback, error);
            }

            @Override
            public void cancel(CallbackStream<C> stream, Callback<C> callback,
                    CancelReason reason) throws Exception {
                reducer.cancel(stream, callback, reason);
            }

            @Override
            public void done(int successful, int failed, int cancelled)
                    throws Exception {
                if (!AbstractCallback.this.isInitialized())
                    return;

                try {
                    AbstractCallback.this.finish(reducer.done(successful, failed, cancelled));
                } catch(Throwable error) {
                    AbstractCallback.this.fail(error);
                }
            }
        };

        return register(new CallbackStream<C>(queries, handle));
    }

    @Override
    public <C> Callback<C> transform(final Transformer<T, C> transformer) {
        final Callback<C> callback = newCallback();

        register(new Handle<T>() {
            @Override
            public void cancel(CancelReason reason) throws Exception {
                callback.cancel(reason);
            }

            @Override
            public void error(Throwable e) throws Exception {
                callback.fail(e);
            }

            @Override
            public void finish(T result) throws Exception {
                try {
                    transformer.transform(result, callback);
                } catch (Throwable t) {
                    callback.fail(t);
                }
            }
        });

        callback.register(new Callback.Handle<C>() {
            @Override
            public void cancel(CancelReason reason) throws Exception {
                AbstractCallback.this.cancel(reason);
            }

            @Override
            public void error(Throwable e) throws Exception {
                AbstractCallback.this.fail(e);
            }

            @Override
            public void finish(C result) throws Exception {}
        });

        return callback;
    }

    @Override
    public Callback<T> resolve(final Executor executor, final Resolver<T> resolver) {
        executor.execute(new Runnable() {
            @Override
            public void run() {
                if (!AbstractCallback.this.isInitialized())
                    return;

                try {
                    AbstractCallback.this.finish(resolver.run());
                } catch(Throwable error) {
                    AbstractCallback.this.fail(error);
                }
            }
        });

        return this;
    }

    public abstract <C> Callback<C> newCallback();
}
