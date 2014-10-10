package com.spotify.heroic.async;

import java.util.List;
import java.util.concurrent.Executor;

import com.spotify.heroic.async.exceptions.CancelledException;
import com.spotify.heroic.async.exceptions.FailedException;

public final class Futures {
    /**
     * Helper functions.
     */

    /**
     * Creates a new concurrent callback using the specified resolver.
     * 
     * @see {@link Future#resolve(Executor, com.spotify.heroic.async.Future.Resolver)}
     */
    public static <C> Future<C> resolve(Executor executor, final Resolver<C> resolver) {
        return new ConcurrentFuture<C>().resolve(executor, resolver);
    }

    /**
     * Creates a new concurrent callback using the specified reducer.
     * 
     * @see {@link Future#reduce(List, com.spotify.heroic.async.Future.Reducer)}
     */
    public static <C, T> Future<T> reduce(List<Future<C>> queries, final Reducer<C, T> reducer) {
        return new ConcurrentFuture<T>().reduce(queries, reducer);
    }

    /**
     * Creates a new concurrent callback using the specified stream reducer.
     * 
     * @see {@link Future#reduce(List, com.spotify.heroic.async.Future.StreamReducer)}
     */
    public static <C, T> Future<T> reduce(List<Future<C>> queries, final StreamReducer<C, T> reducer) {
        return new ConcurrentFuture<T>().reduce(queries, reducer);
    }

    public static <T> Future<T> future() {
        return new ConcurrentFuture<T>();
    }

    /**
     * A callback which has already been resolved as 'resolved'.
     *
     * @author udoprog
     *
     * @param <T>
     */
    private static class ResolvedFuture<T> implements Future<T> {
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

    public static <T> Future<T> resolved(T value) {
        return new ResolvedFuture<T>(value);
    }

    /**
     * A callback which has already been resolved as 'failed'.
     *
     * @author udoprog
     *
     * @param <T>
     */
    private static class FailedFuture<T> implements Future<T> {
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

    public static <T> Future<T> failed(Exception e) {
        return new FailedFuture<T>(e);
    }

    /**
     * A callback which has already been resolved as 'cancelled'.
     *
     * @author udoprog
     *
     * @param <T>
     */
    private static class CancelledFuture<T> implements Future<T> {
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

    public static <T> Future<T> cancelled(CancelReason reason) {
        return new CancelledFuture<T>(reason);
    }
}
