package com.spotify.heroic.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;

import com.spotify.heroic.async.exceptions.CancelledException;
import com.spotify.heroic.async.exceptions.FailedException;
import com.spotify.heroic.async.exceptions.MultiException;

/**
 * Functions to operate with and on futures.
 *
 * Most of these have their equivalent in the Future type directly, where a lot of the arguments are implied in the
 * structure of the future being operated on.
 *
 * @author udoprog
 * @see Future
 */
@Slf4j
public final class Futures {
    /**
     * Helper functions.
     */

    /**
     * @see Future#transform(DelayedTransform)
     */
    public static <C, T> Future<T> transform(final Future<C> future, final DelayedTransform<C, T> transformer,
            final Future<T> target) {
        future.register(new FutureHandle<C>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                target.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                target.fail(e);
            }

            @Override
            public void resolved(C result) throws Exception {
                final Future<T> transform = transformer.transform(result);

                target.register(new FutureHandle<T>() {
                    @Override
                    public void cancelled(CancelReason reason) throws Exception {
                        transform.cancel(reason);
                    }

                    @Override
                    public void failed(Exception e) throws Exception {
                        transform.fail(e);
                    }

                    @Override
                    public void resolved(T result) throws Exception {
                    }
                });

                transform.register(new FutureHandle<T>() {
                    @Override
                    public void cancelled(CancelReason reason) throws Exception {
                        target.cancel(reason);
                    }

                    @Override
                    public void failed(Exception e) throws Exception {
                        target.fail(e);
                    }

                    @Override
                    public void resolved(T result) throws Exception {
                        target.resolve(result);
                    }
                });
            }
        });

        target.register(new FutureHandle<T>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                future.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                future.fail(e);
            }

            @Override
            public void resolved(T result) throws Exception {
            }
        });

        return target;
    }

    public static <C, T> Future<T> transform(final Future<C> future, final Transform<C, T> transformer,
            final ErrorTransformer<T> error, final Future<T> target) {
        future.register(new FutureHandle<C>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                target.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                if (error == null) {
                    target.fail(e);
                    return;
                }

                // use an error transformer.
                target.resolve(error.transform(e));
            }

            @Override
            public void resolved(C result) throws Exception {
                target.resolve(transformer.transform(result));
            }
        });

        target.register(new FutureHandle<T>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                future.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                future.fail(e);
            }

            @Override
            public void resolved(T result) throws Exception {
            }
        });

        return target;
    }

    /**
     * Creates a new concurrent callback using the specified resolver.
     * 
     * @see {@link Future#resolve(Executor, com.spotify.heroic.async.Future.Resolver)}
     */
    public static <C> Future<C> resolve(Executor executor, final Resolver<C> resolver) {
        return resolve(executor, resolver, new ConcurrentFuture<C>());
    }

    public static <C> Future<C> resolve(Executor executor, final Resolver<C> resolver, final Future<C> future) {
        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                if (!future.isReady())
                    return;

                final C result;

                try {
                    result = resolver.resolve();
                } catch (final Exception error) {
                    future.fail(error);
                    return;
                }

                future.resolve(result);
            }
        };

        try {
            executor.execute(runnable);
        } catch (final Exception e) {
            future.fail(e);
        }

        return future;
    }

    /**
     * Creates a new concurrent callback using the specified reducer.
     * 
     * @see {@link Future#reduce(List, com.spotify.heroic.async.Future.Reducer)}
     */
    public static <C, T> Future<T> reduce(List<Future<C>> queries, final Reducer<C, T> reducer) {
        return reduce(queries, reducer, new ConcurrentFuture<T>());
    }

    public static <C, T> Future<T> reduce(List<Future<C>> queries, final ErrorReducer<C, T> reducer) {
        return reduce(queries, reducer, new ConcurrentFuture<T>());
    }

    /**
     * Creates a new concurrent callback using the specified stream reducer.
     * 
     * @see {@link Future#reduce(List, com.spotify.heroic.async.Future.StreamReducer)}
     */
    public static <C, T> Future<T> reduce(List<Future<C>> queries, final StreamReducer<C, T> reducer) {
        return reduce(queries, reducer, new ConcurrentFuture<T>());
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
        public <C> Future<T> reduce(List<Future<C>> callbacks, ErrorReducer<C, T> error) {
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
                log.warn("Failed to call handle finish callback", e);
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
                log.warn("Failed to call finish callback", e);
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
        public <C> Future<C> transform(DelayedTransform<T, C> transformer) {
            try {
                return transformer.transform(value);
            } catch (final Exception e) {
                return new FailedFuture<C>(e);
            }
        }

        @Override
        public <C> Future<C> transform(Transform<T, C> transformer) {
            return transform(transformer, null);
        }

        @Override
        public <C> Future<C> transform(Transform<T, C> transformer, ErrorTransformer<C> error) {
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
        public <C> Future<T> reduce(List<Future<C>> callbacks, ErrorReducer<C, T> error) {
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
                log.warn("Failed to call handle finish callback", e);
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
                log.warn("Failed to call finish callback", e);
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
        public <C> Future<C> transform(DelayedTransform<T, C> transformer) {
            return new FailedFuture<C>(error);
        }

        @Override
        public <C> Future<C> transform(Transform<T, C> transformer) {
            return new FailedFuture<C>(error);
        }

        @Override
        public <C> Future<C> transform(Transform<T, C> transformer, ErrorTransformer<C> error) {
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
        public <C> Future<T> reduce(List<Future<C>> callbacks, ErrorReducer<C, T> error) {
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
                log.warn("Failed to call handle finish callback", e);
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
                log.warn("Failed to call finish callback", e);
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
        public <C> Future<C> transform(DelayedTransform<T, C> transformer) {
            return new CancelledFuture<C>(reason);
        }

        @Override
        public <C> Future<C> transform(Transform<T, C> transformer) {
            return new CancelledFuture<C>(reason);
        }

        @Override
        public <C> Future<C> transform(Transform<T, C> transformer, ErrorTransformer<C> error) {
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

    /**
     * Implementation of {@link Future#reduce(List, Reducer)}.
     *
     * @author udoprog
     *
     * @param <T>
     */
    private static class ReduceImpl<T> implements Cancellable {
        public static interface Handle<T> {
            void done(Collection<T> results, Collection<Exception> errors, Collection<CancelReason> cancelled)
                    throws Exception;
        }

        private final AtomicInteger countdown;
        private final List<Future<T>> callbacks;
        private final Handle<T> handle;
        private volatile boolean done = false;

        private final Queue<Exception> errors = new ConcurrentLinkedQueue<Exception>();
        private final Queue<T> results = new ConcurrentLinkedQueue<T>();
        private final Queue<CancelReason> cancelled = new ConcurrentLinkedQueue<CancelReason>();

        private final FutureHandle<T> listener = new FutureHandle<T>() {
            @Override
            public void failed(Exception e) throws Exception {
                errors.add(e);
                ReduceImpl.this.checkIn();
            }

            @Override
            public void resolved(T result) throws Exception {
                if (result == null)
                    throw new NullPointerException(
                            "CallbackGroup cannot handle null results (due to using a Queue for storing results)");

                results.add(result);
                ReduceImpl.this.checkIn();
            }

            @Override
            public void cancelled(CancelReason reason) throws Exception {
                cancelled.add(reason);
                ReduceImpl.this.checkIn();
            }
        };

        public ReduceImpl(Collection<Future<T>> callbacks, Handle<T> handle) {
            this.countdown = new AtomicInteger(callbacks.size());
            this.callbacks = new ArrayList<Future<T>>(callbacks);
            this.handle = handle;
            this.done = false;

            for (final Future<T> callback : callbacks)
                callback.register(listener);

            if (this.callbacks.isEmpty())
                end();
        }

        /**
         * Checks in a call back. It also wraps up the group if all the callbacks have checked in.
         */
        private void checkIn() {
            final int value = countdown.decrementAndGet();

            if (value != 0)
                return;

            end();
        }

        private synchronized void end() {
            if (done)
                return;

            try {
                handle.done(results, errors, cancelled);
            } catch (final Exception e) {
                log.warn("Failed to call Handle#done", e);
            }

            done = true;
        }

        /* cancel all queries in this group */
        @Override
        public void cancelled(CancelReason reason) {
            for (final Future<T> callback : callbacks) {
                callback.cancel(reason);
            }
        }
    }

    public static <C, T> Future<T> reduce(List<Future<C>> queries, final Reducer<C, T> reducer, final Future<T> target) {
        final ReduceImpl.Handle<C> handle = new ReduceImpl.Handle<C>() {
            @Override
            public void done(Collection<C> results, Collection<Exception> errors, Collection<CancelReason> cancelled)
                    throws Exception {
                if (!target.isReady()) {
                    return;
                }

                if (!errors.isEmpty()) {
                    target.fail(MultiException.combine(errors));
                    return;
                }

                try {
                    target.resolve(reducer.resolved(results, cancelled));
                } catch (final Exception error) {
                    target.fail(error);
                }
            }
        };

        return target.register(new ReduceImpl<C>(queries, handle));
    }

    public static <C, T> Future<T> reduce(List<Future<C>> queries, final ErrorReducer<C, T> reducer,
            final Future<T> target) {
        final ReduceImpl.Handle<C> handle = new ReduceImpl.Handle<C>() {
            @Override
            public void done(Collection<C> results, Collection<Exception> errors, Collection<CancelReason> cancelled)
                    throws Exception {
                if (!target.isReady()) {
                    return;
                }

                try {
                    target.resolve(reducer.reduce(results, cancelled, errors));
                } catch (final Exception error) {
                    target.fail(error);
                }
            }
        };

        return target.register(new ReduceImpl<C>(queries, handle));
    }

    /**
     * Implementation of {@link Future#reduce(List, Future.StreamReducer)}.
     *
     * @author udoprog
     *
     * @param <T>
     */
    private static class StreamReduceImpl<T> implements Cancellable {
        public interface Handle<T> {
            void finish(Future<T> callback, T result) throws Exception;

            void error(Future<T> callback, Exception error) throws Exception;

            void cancel(Future<T> callback, CancelReason reason) throws Exception;

            void done(int successful, int failed, int cancelled) throws Exception;
        }

        private final AtomicInteger countdown;
        private final List<Future<T>> callbacks;
        private final AtomicInteger successful = new AtomicInteger();
        private final AtomicInteger failed = new AtomicInteger();
        private final AtomicInteger cancelled = new AtomicInteger();

        public StreamReduceImpl(Collection<Future<T>> callbacks, final Handle<T> handle) {
            this.countdown = new AtomicInteger(callbacks.size());
            this.callbacks = new ArrayList<Future<T>>(callbacks);

            for (final Future<T> callback : callbacks) {
                callback.register(new FutureHandle<T>() {
                    @Override
                    public void failed(Exception e) throws Exception {
                        failed.incrementAndGet();
                        handleError(handle, callback, e);
                        StreamReduceImpl.this.check(handle);
                    }

                    @Override
                    public void resolved(T result) throws Exception {
                        successful.incrementAndGet();
                        handleFinish(handle, callback, result);
                        StreamReduceImpl.this.check(handle);
                    }

                    @Override
                    public void cancelled(CancelReason reason) throws Exception {
                        cancelled.incrementAndGet();
                        handleCancel(handle, callback, reason);
                        StreamReduceImpl.this.check(handle);
                    }
                });
            }

            if (callbacks.isEmpty())
                handleDone(handle);
        }

        private void handleError(Handle<T> handle, Future<T> callback, Exception error) {
            try {
                handle.error(callback, error);
            } catch (final Exception t) {
                log.warn("Failed to call error on handle", t);
            }
        }

        private void handleFinish(Handle<T> handle, Future<T> callback, T result) {
            try {
                handle.finish(callback, result);
            } catch (final Exception t) {
                log.warn("Failed to call finish on handle", t);
            }
        }

        private void handleCancel(Handle<T> handle, Future<T> callback, CancelReason reason) {
            try {
                handle.cancel(callback, reason);
            } catch (final Exception t) {
                log.warn("Failed to call cancel on handle", t);
            }
        }

        private void handleDone(Handle<T> handle) {
            try {
                handle.done(successful.get(), failed.get(), cancelled.get());
            } catch (final Throwable t) {
                log.warn("Failed to call done on handle", t);
            }
        }

        private void check(Handle<T> handle) throws Exception {
            final int value = countdown.decrementAndGet();

            if (value != 0)
                return;

            handleDone(handle);
        }

        /* cancel all queries in this group */
        @Override
        public void cancelled(CancelReason reason) {
            for (final Future<T> callback : callbacks) {
                callback.cancel(reason);
            }
        }
    }

    public static <C, T> Future<T> reduce(List<Future<C>> queries, final StreamReducer<C, T> reducer,
            final Future<T> target) {
        final StreamReduceImpl.Handle<C> handle = new StreamReduceImpl.Handle<C>() {
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
                if (!target.isReady())
                    return;

                try {
                    target.resolve(reducer.resolved(successful, failed, cancelled));
                } catch (final Exception error) {
                    target.fail(error);
                }
            }
        };

        return target.register(new StreamReduceImpl<C>(queries, handle));
    }

    /**
     * Create a bi-directional link between future <code>a</code> and future <code>b</code>.
     * 
     * If either is resolved, failed, or cancelled. The other one will be notified to the same effect.
     * 
     * @param a
     *            First future to link.
     * @param b
     *            Second future to link.
     * @return The second future (after it's been linked).
     */
    public static <C> Future<C> link(final Future<C> a, final Future<C> b) {
        a.register(new FutureHandle<C>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                b.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                b.fail(e);
            }

            @Override
            public void resolved(C result) throws Exception {
                b.resolve(result);
            }
        });

        return b.register(new FutureHandle<C>() {
            @Override
            public void cancelled(CancelReason reason) throws Exception {
                a.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                a.fail(e);
            }

            @Override
            public void resolved(C result) throws Exception {
                a.resolve(result);
            }
        });
    }
}
