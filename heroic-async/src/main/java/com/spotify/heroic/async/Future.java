package com.spotify.heroic.async;

import java.util.List;
import java.util.concurrent.Executor;

import com.spotify.heroic.async.exceptions.CancelledException;
import com.spotify.heroic.async.exceptions.FailedException;

/**
 * Interface for asynchronous callbacks with the ability to subscribe to interesting events.
 *
 * The available events are.
 *
 * <ul>
 * <li>resolved, for when a callback has been resolved with a value.</li>
 * <li>failed, for when a callback failed to resolve because of an exception.</li>
 * <li>cancelled, for when a callback will not resolve, with a reason {@link CancelReason}.</li>
 * </ul>
 *
 * @author udoprog
 *
 * @param <T>
 *            The type being realized in the callback's finish method.
 */
public interface Future<T> {
    public static enum State {
        // state when it's not been resolved, failed or cancelled.
        READY,
        // every other state.
        FAILED, RESOLVED, CANCELLED
    }

    public Future<T> cancel(CancelReason reason);

    public Future<T> fail(Exception error);

    public Future<T> resolve(T result);

    /**
     * Resolve this callback asynchronously using the resolver. This is a common pattern which is provided because the
     * implementation details are important.
     *
     * 1. Since anything scheduled on an executor is viable for execution 'later', the callback might have already been
     * resolved or cancelled. It is therefore important to check {@link #isReady()} before calling {@link #resolve(T)}.
     *
     * 2. Since the resolving context might throw an exception, it is important that this caught and that the callback
     * is marked as 'failed' appropriately by calling {@link #fail(Exception)}.
     *
     * If you are willing to ensure these two behaviors, the Executor can be used directly.
     *
     * @param executor
     *            Executor to schedule the resolver task on.
     * @param resolver
     *            The resolver to use for resolving this callback.
     * @return This callback.
     */
    public Future<T> resolve(Executor executor, Resolver<T> resolver);

    /**
     * Register functions to be fired for any of the possible events for a callback.
     *
     * These events are; cancelled, failed or resolved.
     *
     * @param handle
     *            Contains functions to be fired.
     * @return This callback.
     */
    public Future<T> register(FutureHandle<T> handle);

    /**
     * Same as {@link #register(Handle<T>)}, but for Handle<Object> types which don't care for the result of the
     * operation, only the events that happens.
     *
     * @param handle
     *            Contains functions to be fired.
     * @return This callback.
     */
    public Future<T> register(ObjectHandle handle);

    /**
     * Register a function to be fired if this callback is finished (either cancelled or failed).
     *
     * @param finishable
     *            Function to be fired.
     * @return This callback.
     */
    public Future<T> register(Finishable finishable);

    /**
     * Register a function to be fired if this callback is cancelled.
     *
     * @param cancellable
     *            Function to be fired.
     * @return This callback.
     */
    public Future<T> register(Cancellable cancellable);

    /**
     * Make this callback depend on another and vice-versa.
     *
     * @param callback
     *            Callback to depend on.
     * @return This callback.
     */
    public Future<T> register(Future<T> callback);

    /**
     * Check if callback is ready.
     *
     * If not, any operation associated with the callback should be avoided as much as possible, because;
     *
     * a) A result is already available. b) Some other required part of the computation failed, meaning that a result is
     * useless anyways. c) The request was cancelled.
     *
     * @return Boolean indiciating if this callback is ready or not.
     */
    public boolean isReady();

    /**
     * Resolve the value of a callback using a collection of callbacks.
     *
     * <pre>
     * List<Callback<C>> - *using reducer* -> Callback<T> (this)
     * </pre>
     *
     * The group will be connected to this callback in that it's result will finish this callback and any cancellations
     * of this callback will cancel the entire group.
     *
     * <pre>
     * {@code
     *   List<Callback<Integer>> callbacks = asyncListOperation();
     * 
     *   Callback<Integer> callback = ConcurrentCallback.newReduce(callbacks, Callback.Reducer<Integer, Integer>() {
     *     Integer resolved(Collection<Integer> results, Collection<Exception> errors, Collection<CancelReason> cancelled) {
     *       return sum(results);
     *     }
     *   }
     * 
     *   # use callback
     * }
     * </pre>
     *
     * @param callbacks
     *            Collection of callbacks to reduce.
     * @param reducer
     *            Function responsible for reducing the collection into a single object.
     * @return A new callback with the generic value <T>.
     */
    public <C> Future<T> reduce(List<Future<C>> callbacks, final Reducer<C, T> reducer);

    /**
     * Resolve the value of a callback using a collection of callbacks. Similar to {@link #reduce(List, Reducer)} but
     * using the {@link StreamReducer} to receive results as they arrive. The benefit of a streamed approach is that
     * intermittent results do not have to be kept in memory.
     *
     * <pre>
     * {@code
     * List<Callback<C>> - *using stream reducer* -> Callback<T> (this)
     * }
     * </pre>
     *
     * The group will be connected to this callback in that it's result will finish this callback. Any cancellations of
     * this callback will cancel the entire collection of callbacks.
     *
     * <pre>
     * {@code
     *   List<Callback<Integer>> callbacks = asyncListOperation();
     * 
     *   Callback<Integer> callback = ConcurrentCallback.newReduce(callbacks, new Callback.StreamReducer<Integer, Integer>() {
     *     final AtomicInteger value = new AtomicInteger(0);
     * 
     *     void finish(Callback<Integer> callback, Integer result) {
     *       value.addAndGet(result);
     *     }
     * 
     *     void failed(Callback<Integer> callback, Exception error) {
     *     }
     * 
     *     void cancel(Callback<Integer> callback, CancelReason reason) throws Exception {
     *     }
     * 
     *     Double resolved(int successful, int failed, int cancelled) throws Exception {
     *       return result.get();
     *     }
     *   });
     * 
     *   # use callback
     * }
     * </pre>
     *
     * @param callbacks
     *            Collection of callbacks to reduce.
     * @param reducer
     *            Function responsible for reducing the collection into a single object.
     * @return A new callback with the generic value <T>.
     */
    public <C> Future<T> reduce(List<Future<C>> callbacks, final StreamReducer<C, T> reducer);

    /**
     * Transforms the value of one callback into another using a deferred transformer function.
     *
     * <pre>
     * Callback<T> (this) - *using deferred transformer* -> Callback<C>
     * </pre>
     *
     * A deferred transformer is expected to return a compatible callback that when resolved will resolve the callback
     * that this function returns.
     *
     * <pre>
     * {@code
     *   Callback<Integer> first = asyncOperation();
     * 
     *   Callback<Double> second = callback.transform(new Transformer<Integer, Double>() {
     *     void transform(Integer result, Callback<Double> callback) {
     *       callback.finish(result.doubleValue());
     *     }
     *   };
     * 
     *   # use second
     * }
     * </pre>
     *
     * @param transformer
     *            The function to use when transforming the value.
     * @return A callback of type <C> which resolves with the transformed value.
     */
    public <C> Future<C> transform(DeferredTransformer<T, C> transformer);

    /**
     * Transforms the value of this callback into another type using a transformer function.
     *
     * <pre>
     * Callback<T> (this) - *using transformer* -> Callback<C>
     * </pre>
     *
     * Use this if the transformation performed does not require any more async operations.
     *
     * <pre>
     * {@code
     *   Callback<Integer> first = asyncOperation();
     * 
     *   Callback<Double> second = callback.transform(new Transformer<Integer, Double>() {
     *     Double transform(Integer result) {
     *       return result.doubleValue();
     *     }
     *   };
     * 
     *   # use second
     * }
     * </pre>
     *
     * @param transformer
     * @return
     */
    public <C> Future<C> transform(Transformer<T, C> transformer);

    public <C> Future<C> transform(Transformer<T, C> transformer, ErrorTransformer<C> error);

    /**
     * Block until result is available.
     *
     * @return The result of this callback being resolved.
     * @throws Exception
     *             If the callback being resolved threw an exception.
     */
    public T get() throws InterruptedException, CancelledException, FailedException;
}
