package com.spotify.heroic.async;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Provide some common implementations of a callback.
 *
 * @author udoprog
 *
 * @param <T>
 *            The value type of the callback.
 */
abstract class AbstractFuture<T> implements Future<T> {
    @Override
    public <C> Future<T> reduce(List<Future<C>> queries, final Reducer<C, T> reducer) {
        final FutureReducer.Handle<C> handle = new FutureReducer.Handle<C>() {
            @Override
            public void done(Collection<C> results, Collection<Exception> errors, Collection<CancelReason> cancelled)
                    throws Exception {
                if (!AbstractFuture.this.isReady())
                    return;

                try {
                    AbstractFuture.this.resolve(reducer.resolved(results, errors, cancelled));
                } catch (final Exception error) {
                    AbstractFuture.this.fail(error);
                }
            }
        };

        return register(new FutureReducer<C>(queries, handle));
    }

    @Override
    public <C> Future<T> reduce(List<Future<C>> queries, final StreamReducer<C, T> reducer) {
        final StreamReducerCallback<C> handle = new StreamReducerCallback<C>() {
            @Override
            public void finish(Future<C> callback, C result) throws Exception {
                reducer.resolved(callback, result);
            }

            @Override
            public void error(Future<C> callback, Exception error) throws Exception {
                reducer.failed(callback, error);
            }

            @Override
            public void cancel(Future<C> callback, CancelReason reason) throws Exception {
                reducer.cancelled(callback, reason);
            }

            @Override
            public void done(int successful, int failed, int cancelled) throws Exception {
                if (!AbstractFuture.this.isReady())
                    return;

                try {
                    AbstractFuture.this.resolve(reducer.resolved(successful, failed, cancelled));
                } catch (final Exception error) {
                    AbstractFuture.this.fail(error);
                }
            }
        };

        return register(new StreamReducerImplementation<C>(queries, handle));
    }

    @Override
    public <C> Future<C> transform(final DeferredTransformer<T, C> transformer) {
        final Future<C> callback = newCallback();

        register(new FutureHandle<T>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                callback.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                callback.fail(e);
            }

            @Override
            public void resolved(T result) throws Exception {
                final Future<C> transform = transformer.transform(result);

                callback.register(new FutureHandle<C>() {
                    @Override
                    public void cancelled(CancelReason reason) throws Exception {
                        transform.cancel(reason);
                    }

                    @Override
                    public void failed(Exception e) throws Exception {
                        transform.fail(e);
                    }

                    @Override
                    public void resolved(C result) throws Exception {
                        transform.resolve(result);
                    }
                });

                transform.register(new FutureHandle<C>() {
                    @Override
                    public void cancelled(CancelReason reason) throws Exception {
                        callback.cancel(reason);
                    }

                    @Override
                    public void failed(Exception e) throws Exception {
                        callback.fail(e);
                    }

                    @Override
                    public void resolved(C result) throws Exception {
                        callback.resolve(result);
                    }
                });
            }
        });

        callback.register(new FutureHandle<C>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                AbstractFuture.this.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                AbstractFuture.this.fail(e);
            }

            @Override
            public void resolved(C result) throws Exception {
            }
        });

        return callback;
    }

    @Override
    public <C> Future<C> transform(final Transformer<T, C> transformer) {
        return transform(transformer, null);
    }

    @Override
    public <C> Future<C> transform(final Transformer<T, C> transformer, final ErrorTransformer<C> error) {
        final Future<C> callback = newCallback();

        register(new FutureHandle<T>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                callback.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                if (error == null) {
                    callback.fail(e);
                    return;
                }

                // use an error transformer.
                try {
                    callback.resolve(error.transform(e));
                } catch (final Exception e2) {
                    callback.fail(e2);
                }
            }

            @Override
            public void resolved(T result) throws Exception {
                try {
                    callback.resolve(transformer.transform(result));
                } catch (final Exception e2) {
                    callback.fail(e2);
                }
            }
        });

        callback.register(new FutureHandle<C>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                AbstractFuture.this.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                AbstractFuture.this.fail(e);
            }

            @Override
            public void resolved(C result) throws Exception {
            }
        });

        return callback;
    }

    @Override
    public Future<T> resolve(final Executor executor, final Resolver<T> resolver) {
        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                if (!AbstractFuture.this.isReady())
                    return;

                try {
                    AbstractFuture.this.resolve(resolver.resolve());
                } catch (final Exception error) {
                    AbstractFuture.this.fail(error);
                }
            }
        };

        try {
            executor.execute(runnable);
        } catch (final Exception e) {
            AbstractFuture.this.fail(e);
        }

        return this;
    }

    @Override
    public Future<T> register(final Future<T> callback) {
        register(new FutureHandle<T>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                callback.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                callback.fail(e);
            }

            @Override
            public void resolved(T result) throws Exception {
                callback.resolve(result);
            }
        });

        callback.register(new FutureHandle<T>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                AbstractFuture.this.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                AbstractFuture.this.fail(e);
            }

            @Override
            public void resolved(T result) throws Exception {
                AbstractFuture.this.resolve(result);
            }
        });

        return this;
    }

    protected abstract <C> Future<C> newCallback();
}
