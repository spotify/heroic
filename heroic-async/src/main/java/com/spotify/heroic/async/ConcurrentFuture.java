package com.spotify.heroic.async;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import com.spotify.heroic.async.exceptions.CancelledException;
import com.spotify.heroic.async.exceptions.FailedException;

/**
 * A class implementing the callback pattern concurrently in a way that any thread can use the callback instance in a
 * thread-safe manner.
 * 
 * The callback will retain it's result if it arrives early allowing for graceful additions of late listeners. This
 * allows for the deferred work to start immediately.
 * 
 * It also allows for cancellation in any order.
 * 
 * <h1>Example</h1>
 * 
 * <pre>
 * {@code
 *     Callback<Integer> callback = new ConcurrentCallback<Integer>();
 *     new Thread(new Runnable() callback.resolve(12); }).start();
 *     callback.listen(new Callback.Handle<T>() { ... });
 * }
 * </pre>
 * 
 * <h1>Synchronized functions.</h3>
 * 
 * rule a) The following fields must only be accessed in a block synchronized on <code>state</code>: state, error,
 * cancelReason, result, handlers, cancellables, and finishables.
 * 
 * rule b) No other callback must be invoked in a synchronized block since that could result in deadlocks because _this_
 * callback could be part of another callbacks chain.
 * 
 * @author udoprog
 * 
 * @param <T>
 *            The type being deferred.
 */
class ConcurrentFuture<T> implements Future<T> {
    private List<FutureHandle<T>> handlers = new LinkedList<FutureHandle<T>>();
    private List<Cancellable> cancellables = new LinkedList<Cancellable>();
    private List<Finishable> finishables = new LinkedList<Finishable>();

    private volatile State state = State.READY;

    private Exception error;
    private CancelReason cancelReason;
    private T result;

    @Override
    public Future<T> fail(Exception error) {
        if (state != State.READY)
            return this;

        synchronized (state) {
            if (state != State.READY)
                return this;

            this.state = State.FAILED;
            this.error = error;
        }

        for (final FutureHandle<T> handle : handlers)
            callFailed(handle);

        for (final Finishable finishable : finishables)
            callFinished(finishable);

        clear();
        return this;
    }

    @Override
    public Future<T> resolve(T result) {
        if (state != State.READY)
            return this;

        synchronized (state) {
            if (state != State.READY)
                return this;

            this.state = State.RESOLVED;
            this.result = result;
        }

        for (final FutureHandle<T> handle : handlers)
            callResolved(handle);

        for (final Finishable finishable : finishables)
            callFinished(finishable);

        clear();

        return this;
    }

    @Override
    public Future<T> cancel(CancelReason reason) {
        if (state != State.READY)
            return this;

        synchronized (state) {
            if (state != State.READY)
                return this;

            this.state = State.CANCELLED;
            this.cancelReason = reason;
        }

        for (final FutureHandle<T> handle : handlers)
            callCancelled(handle);

        for (final Cancellable cancellable : cancellables)
            callCancelled(cancellable);

        for (final Finishable finishable : finishables)
            callFinished(finishable);

        clear();

        return this;
    }

    @Override
    public Future<T> register(FutureHandle<T> handle) {
        switch (addHandler(handle)) {
        case RESOLVED:
            callResolved(handle);
            break;
        case CANCELLED:
            callCancelled(handle);
            break;
        case FAILED:
            callFailed(handle);
            break;
        default:
            break;
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
        if (addFinishable(finishable) != State.READY)
            callFinished(finishable);

        return this;
    }

    @Override
    public Future<T> register(Cancellable cancellable) {
        if (addCancellable(cancellable) == State.CANCELLED)
            callCancelled(cancellable);

        return this;
    }

    @Override
    public boolean isReady() {
        return state == State.READY;
    }

    /**
     * Make a point to clear all handles to assert that any associated memory can be freed up ASAP.
     */
    private void clear() {
        handlers = null;
        cancellables = null;
        finishables = null;
    }

    private void callResolved(FutureHandle<T> handle) {
        try {
            handle.resolved(result);
        } catch (final Exception e) {
            throw new RuntimeException("Handle#resolved(T): failed", e);
        }
    }

    private void callFailed(FutureHandle<T> handle) {
        try {
            handle.failed(error);
        } catch (final Exception e) {
            throw new RuntimeException("Handle#failed(Exception): failed", e);
        }
    }

    private void callFinished(Finishable finishable) {
        try {
            finishable.finished();
        } catch (final Exception e) {
            throw new RuntimeException("Finishable#finished(): failed", e);
        }
    }

    private void callCancelled(Cancellable cancellable) {
        try {
            cancellable.cancelled(cancelReason);
        } catch (final Exception e) {
            throw new RuntimeException("Cancellable#cancelled(CancelReason): failed", e);
        }
    }

    private void callCancelled(FutureHandle<T> handle) {
        try {
            handle.cancelled(cancelReason);
        } catch (final Exception e) {
            throw new RuntimeException("Cancellable#cancelled(CancelReason): failed", e);
        }
    }

    private State addHandler(FutureHandle<T> handle) {
        synchronized (state) {
            if (state != State.READY)
                return state;

            handlers.add(handle);
        }

        return State.READY;
    }

    private State addCancellable(Cancellable cancellable) {
        synchronized (state) {
            if (state != State.READY)
                return state;

            cancellables.add(cancellable);
        }

        return State.READY;
    }

    private State addFinishable(Finishable finishable) {
        synchronized (state) {
            if (state != State.READY)
                return state;

            finishables.add(finishable);
        }

        return State.READY;
    }

    @Override
    public T get() throws InterruptedException, CancelledException, FailedException {
        final CountDownLatch latch = new CountDownLatch(1);

        register(new Finishable() {
            @Override
            public void finished() throws Exception {
                latch.countDown();
            }
        });

        latch.await();

        switch (state) {
        case CANCELLED:
            throw new CancelledException(cancelReason);
        case FAILED:
            throw new FailedException(error);
        case RESOLVED:
            return result;
        default:
            throw new IllegalStateException(state.toString());
        }
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
                throw new RuntimeException("Failed to call Handle#done", e);
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
                throw new RuntimeException("Failed to call error on handle", t);
            }
        }

        private void handleFinish(Handle<T> handle, Future<T> callback, T result) {
            try {
                handle.finish(callback, result);
            } catch (final Exception t) {
                throw new RuntimeException("Failed to call finish on handle", t);
            }
        }

        private void handleCancel(Handle<T> handle, Future<T> callback, CancelReason reason) {
            try {
                handle.cancel(callback, reason);
            } catch (final Exception t) {
                throw new RuntimeException("Failed to call cancel on handle", t);
            }
        }

        private void handleDone(Handle<T> handle) {
            try {
                handle.done(successful.get(), failed.get(), cancelled.get());
            } catch (final Throwable t) {
                throw new RuntimeException("Failed to call done on handle", t);
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

    @Override
    public <C> Future<T> reduce(List<Future<C>> queries, final Reducer<C, T> reducer) {
        final ReduceImpl.Handle<C> handle = new ReduceImpl.Handle<C>() {
            @Override
            public void done(Collection<C> results, Collection<Exception> errors, Collection<CancelReason> cancelled)
                    throws Exception {
                if (!ConcurrentFuture.this.isReady())
                    return;

                try {
                    ConcurrentFuture.this.resolve(reducer.resolved(results, errors, cancelled));
                } catch (final Exception error) {
                    ConcurrentFuture.this.fail(error);
                }
            }
        };

        return register(new ReduceImpl<C>(queries, handle));
    }

    @Override
    public <C> Future<T> reduce(List<Future<C>> queries, final StreamReducer<C, T> reducer) {
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
                if (!ConcurrentFuture.this.isReady())
                    return;

                try {
                    ConcurrentFuture.this.resolve(reducer.resolved(successful, failed, cancelled));
                } catch (final Exception error) {
                    ConcurrentFuture.this.fail(error);
                }
            }
        };

        return register(new StreamReduceImpl<C>(queries, handle));
    }

    @Override
    public <C> Future<C> transform(final DeferredTransformer<T, C> transformer) {
        final Future<C> callback = new ConcurrentFuture<>();

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
                ConcurrentFuture.this.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                ConcurrentFuture.this.fail(e);
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
        final Future<C> callback = new ConcurrentFuture<>();

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
                ConcurrentFuture.this.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                ConcurrentFuture.this.fail(e);
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
                if (!ConcurrentFuture.this.isReady())
                    return;

                try {
                    ConcurrentFuture.this.resolve(resolver.resolve());
                } catch (final Exception error) {
                    ConcurrentFuture.this.fail(error);
                }
            }
        };

        try {
            executor.execute(runnable);
        } catch (final Exception e) {
            ConcurrentFuture.this.fail(e);
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
                ConcurrentFuture.this.cancel(reason);
            }

            @Override
            public void failed(Exception e) throws Exception {
                ConcurrentFuture.this.fail(e);
            }

            @Override
            public void resolved(T result) throws Exception {
                ConcurrentFuture.this.resolve(result);
            }
        });

        return this;
    }
}