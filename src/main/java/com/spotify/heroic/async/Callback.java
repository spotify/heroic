package com.spotify.heroic.async;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * Interface for asynchronous callbacks with the ability to subscribe to
 * interesting events.
 * 
 * @author udoprog
 * 
 * @param <T>
 *            The type being realized in the callback's finish method.
 */
public interface Callback<T> {
    public static enum State {
        INITIALIZED, FAILED, FINISHED, CANCELLED
    }

    public static interface Cancellable {
        void cancel(CancelReason reason) throws Exception;
    }

    public static interface Finishable {
        void finish() throws Exception;
    }

    public static interface Handle<T> extends Cancellable {
        void error(Throwable e) throws Exception;

        void finish(T result) throws Exception;
    }

    /**
     * Simplified abstraction on top of CallbackGroup meant to reduce the result
     * of multiple queries into one.
     * 
     * Will be called when the entire result is available. If this is
     * undesirable, use {@link #StreamReducer}.
     * 
     * @author udoprog
     * 
     * @param <C>
     *            The type of callbacks to group.
     * @param <R>
     *            The return type of the callback operation.
     */
    public static interface Reducer<C, R> {
        R done(Collection<C> results, Collection<Throwable> errors,
                Collection<CancelReason> cancelled) throws Exception;
    }

    /**
     * Simplified abstraction on top of CallbackStream meant to reduce the
     * result of multiple queries into one.
     * 
     * Will be called each time a result is available immediately.
     * 
     * @author udoprog
     * 
     * @param <C>
     */
    public static interface StreamReducer<C, R> {
        void finish(CallbackStream<C> stream, Callback<C> callback, C result)
                throws Exception;

        void error(CallbackStream<C> stream, Callback<C> callback,
                Throwable error) throws Exception;

        void cancel(CallbackStream<C> stream, Callback<C> callback,
                CancelReason reason) throws Exception;

        R done(int successful, int failed, int cancelled) throws Exception;
    }

    public static interface Transformer<C, R> {
        void transform(C result, Callback<R> callback) throws Exception;
    }

    public static interface Resolver<R> {
        R run() throws Exception;
    }

    public Callback<T> fail(Throwable error);

    public Callback<T> finish(T result);

    public Callback<T> cancel(CancelReason reason);

    public Callback<T> register(Handle<T> handle);

    public Callback<T> register(Finishable finishable);

    public Callback<T> register(Cancellable cancellable);

    public boolean isInitialized();

    /**
     * Create a new callback group connected to this callback.
     * 
     * The group will be connected to this callback in that it's result will
     * finish this callback and any cancellations of this callback will cancel
     * the entire group.
     * 
     * @param callbacks
     *            Callbacks to group.
     * @param timer
     *            Timer to measure the handle time of the group.
     * @param reducer
     *            reducer is responsible for reducing the given callbacks into
     *            the returned callback
     */
    public <C> Callback<T> reduce(List<Callback<C>> callbacks, final Reducer<C, T> reducer);

    public <C> Callback<T> reduce(List<Callback<C>> callbacks, final StreamReducer<C, T> reducer);

    public <C> Callback<C> transform(Transformer<T, C> transformer);

    public Callback<T> resolve(Executor executor, Resolver<T> resolver);
}
