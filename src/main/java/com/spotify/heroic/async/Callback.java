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
        // state when it's not been resolved, failed or cancelled.
        READY,
        // every other state.
        FAILED, RESOLVED, CANCELLED
    }

    public static interface Cancellable {
        void cancelled(CancelReason reason) throws Exception;
    }

    public static interface Finishable {
        void finished() throws Exception;
    }

    public static interface Handle<T> {
        void cancelled(CancelReason reason) throws Exception;

        void failed(Exception e) throws Exception;

        void resolved(T result) throws Exception;
    }

    public static interface ObjectHandle extends Handle<Object> {
    }

    /**
     * Simplified abstraction on top of CallbackGroup meant to reduce the result
     * of multiple queries into one.
     * 
     * Will be called when the entire result is available. If this is
     * undesirable, use {@link #StreamReducer}.
     * 
     * @author udoprog
     */
    public static interface Reducer<C, R> {
        R resolved(Collection<C> results, Collection<Exception> errors,
                Collection<CancelReason> cancelled) throws Exception;
    }

    public static interface StreamReducer<C, R> {
        /**
         * Implement to trigger on one resolved.
         */
        void resolved(Callback<C> callback, C result) throws Exception;
        /**
         * Implement to trigger on one failed.
         */
        void failed(Callback<C> callback, Exception error) throws Exception;
        /**
         * Implement to trigger on one cancelled.
         */
        void cancelled(Callback<C> callback, CancelReason reason) throws Exception;
        /**
         * Implement to fire when all callbacks have been resolved.
         */
        R resolved(int successful, int failed, int cancelled) throws Exception;
    }
    /**
     * Convenience class that can be used to extend and implement only a subset of the functionality of {@link StreamReducer}.
     *
     * Note: {@link StreamReducer#resolved(int, int, int)} is the minimal required implementation since it is not provided here.
     *
     * @author udoprog
     */
    public static abstract class DefaultStreamReducer<C, R> implements StreamReducer<C, R> {
        /**
         * Override to trigger on one resolved.
         */
        @Override
        public void resolved(Callback<C> callback, C result) throws Exception { }
        /**
         * Override to trigger on one failed.
         */
        @Override
        public void failed(Callback<C> callback, Exception error) throws Exception { }
        /**
         * Override to trigger on one cancelled.
         */
        @Override
        public void cancelled(Callback<C> callback, CancelReason reason) throws Exception { }
    }

    public static interface DeferredTransformer<C, R> {
        Callback<R> transform(C result) throws Exception;
    }

    public static interface Transformer<C, R> {
        R transform(C result) throws Exception;
    }

    public static interface Resolver<R> {
        R resolve() throws Exception;
    }

    public Callback<T> cancel(CancelReason reason);
    public Callback<T> fail(Exception error);
    public Callback<T> resolve(T result);

    /**
     * Resolve this callback asynchronously using the resolver.
     * This is a common pattern which is provided because the implementation details are important.
     *
     * 1. Since anything scheduled on an executor is viable for execution 'later', the callback might have already been resolved or cancelled.
     *    It is therefore important to check {@link #isReady()} before calling the resolved.
     * 2. Since the resolver might throw an exception, it is important that this exception is caught and that the callback is marked as 'failed' appropriately.
     *
     * If you are willing to ensure these two behaviours, the Executor can be used directly.
     *
     * @param executor Executor to schedule the resolver task on.
     * @param resolver The resolver to use for resolving this callback.
     * @return This callback.
     */
    public Callback<T> resolve(Executor executor, Resolver<T> resolver);

    /**
     * Register functions to be fired for any of the possible events for a callback.
     * These are; cancelled, failed or resolved.
     *
     * @param handle Contains functions to be fired.
     * @return This callback.
     */
    public Callback<T> register(Handle<T> handle);

    public Callback<T> register(ObjectHandle handle);

    /**
     * Register a function to be fired if this callback is finished (either cancelled or failed).
     *
     * @param finishable Function to be fired.
     * @return This callback.
     */
    public Callback<T> register(Finishable finishable);

    /**
     * Register a function to be fired if this callback is cancelled.
     * 
     * @param cancellable
     *            Function to be fired.
     * @return This callback.
     */
    public Callback<T> register(Cancellable cancellable);

    /**
     * Make this callback depend on another and vice-versa.
     * 
     * @param callback
     *            Callback to depend on.
     * @return This callback.
     */
    public Callback<T> register(Callback<T> callback);

    /**
     * Check if callback is ready.
     * 
     * If not, any operation associated with the callback should be avoided as
     * much as possible, because;
     * 
     * a) A result is already available. b) Some other required part of the
     * computation failed, meaning that a result is useless anyways. c) The
     * request was cancelled.
     * 
     * @return Boolean indiciating if this callback is ready or not.
     */
    public boolean isReady();

    /**
     * Resolve the value of a callback using a collection of callbacks.
     * 
     * The group will be connected to this callback in that it's result will
     * finish this callback and any cancellations of this callback will cancel
     * the entire group.
     *
     * <pre>
     * {@code
     * final List<Callback<Integer>> callbacks = buildCallbacks();
     * final Callback<Double> callback = new ConcurrentCallback<Double>();
     *
     * callback.reduce(callbacks, new Callback.Reducer<Integer, Double>(){
     *   Double resolved(Collection<Integer> results, Collection<Exception> errors, Collection<CancelReason> cancelled) throws Exception {
     *     double result = 0;
     *
     *     for (final Integer part : results) {
     *       result += part.doubleValue();
     *     }
     *
     *     return result;
     *   }
     * });
     * }
     * </pre>
     *
     * @param callbacks Collection of callbacks to reduce.
     * @param reducer Function responsible for reducing the collection into a single object.
     * @return A new callback with the generic value <T>.
     */
    public <C> Callback<T> reduce(List<Callback<C>> callbacks, final Reducer<C, T> reducer);

    /**
     * Resolve the value of a callback using a collection of callbacks.
     * Similar to {@link #reduce(List, Reducer)} but using the {@link StreamReducer} to receive results as they arrive.
     * The benefit of a streamed approach is that intermittent results do not have to be kept in memory.
     * 
     * The group will be connected to this callback in that it's result will finish this callback.
     * Any cancellations of this callback will cancel the entire collection of callbacks.
     *
     * <pre>
     * {@code
     * final List<Callback<Integer>> callbacks = buildCallbacks();
     * final Callback<Double> callback = new ConcurrentCallback<Double>();
     *
     * callback.reduce(callbacks, new Callback.StreamReducer<Integer, Double>() {
     *   final AtomicDouble value = new AtomicDouble(0);
     *
     *   void finish(Callback<Integer> callback, Integer result) throws Exception {
     *     value.addAndGet(result.doubleValue());
     *   }
     *
     *   void failed(Callback<Integer> callback, Exception error) throws Exception {
     *   }
     *
     *   void cancel(Callback<Integer> callback, CancelReason reason) throws Exception {
     *   }
     *
     *   Double resolved(int successful, int failed, int cancelled) throws Exception {
     *     return result.get();
     *   }
     * });
     * }
     * </pre>
     *
     * @param callbacks Collection of callbacks to reduce.
     * @param reducer Function responsible for reducing the collection into a single object.
     * @return A new callback with the generic value <T>.
     */
    public <C> Callback<T> reduce(List<Callback<C>> callbacks, final StreamReducer<C, T> reducer);

    /**
     * Transforms the value of one callback into another using a transformer function.
     *
     * <pre>
     * {@code
     * final Callback<Integer> callback = new ConcurrentCallback<Integer>();
     * callback.transform(new Transformer<Integer, Double>() {
     *   void transform(Integer result, Callback<Double> callback) throws Exception {
     *     callback.finish(result.doubleValue());
     *   }
     * });
     * }
     * </pre>
     *
     * @param transformer The function to use when transforming the value.
     * @return A callback of type <C> which resolves with the transformed value.
     */
    public <C> Callback<C> transform(DeferredTransformer<T, C> transformer);
    public <C> Callback<C> transform(Transformer<T, C> transformer);
}
