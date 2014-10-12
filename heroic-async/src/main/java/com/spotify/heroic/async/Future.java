package com.spotify.heroic.async;

import java.util.List;
import java.util.concurrent.Executor;

import com.spotify.heroic.async.exceptions.CancelledException;
import com.spotify.heroic.async.exceptions.FailedException;

/**
 * Interface for asynchronous futures with the ability to subscribe to interesting events.
 *
 * The available events are.
 *
 * <ul>
 * <li>resolved, for when a future has been resolved with a value.</li>
 * <li>failed, for when a future failed to resolve because of an exception.</li>
 * <li>cancelled, for when a future will not resolve, with a reason {@link CancelReason}.</li>
 * </ul>
 *
 * @author udoprog
 *
 * @param <T>
 *            The type being realized in the future's finish method.
 */
public interface Future<T> {
    public static enum State {
        // state when it's not been resolved, failed or cancelled.
        READY,
        // every other state.
        FAILED, RESOLVED, CANCELLED,
        // mid-flight finalizing.
        FINALIZING
    }

    public Future<T> cancel(CancelReason reason);

    public Future<T> fail(Exception error);

    public Future<T> resolve(T result);

    /**
     * Resolve this future asynchronously using the resolver. This is a common pattern which is provided because the
     * implementation details are important.
     *
     * 1. Since anything scheduled on an executor is viable for execution 'later', the future might have already been
     * resolved or cancelled. It is therefore important to check {@link #isReady()} before calling {@link #resolve(T)}.
     *
     * 2. Since the resolving context might throw an exception, it is important that this caught and that the future is
     * marked as 'failed' appropriately by calling {@link #fail(Exception)}.
     *
     * If you are willing to ensure these two behaviors, the Executor can be used directly.
     *
     * @param executor
     *            Executor to schedule the resolver task on.
     * @param resolver
     *            The resolver to use for resolving this future.
     * @return This future.
     */
    public Future<T> resolve(Executor executor, Resolver<T> resolver);

    /**
     * Register functions to be fired for any of the possible events for a future.
     *
     * These events are; cancelled, failed or resolved.
     *
     * @param handle
     *            Contains functions to be fired.
     * @return This future.
     */
    public Future<T> register(FutureHandle<T> handle);

    /**
     * Same as {@link #register(Handle<T>)}, but for Handle<Object> types which don't care for the result of the
     * operation, only the events that happens.
     *
     * @param handle
     *            Contains functions to be fired.
     * @return This future.
     */
    public Future<T> register(ObjectHandle handle);

    /**
     * Register a function to be fired if this future is finished (either cancelled or failed).
     *
     * @param finishable
     *            Function to be fired.
     * @return This future.
     */
    public Future<T> register(Finishable finishable);

    /**
     * Register a function to be fired if this future is cancelled.
     *
     * @param cancellable
     *            Function to be fired.
     * @return This future.
     */
    public Future<T> register(Cancellable cancellable);

    /**
     * Make this future depend on another and vice-versa.
     *
     * @param future
     *            Future to depend on.
     * @return This future.
     */
    public Future<T> register(Future<T> future);

    /**
     * Check if future is ready.
     *
     * If not, any operation associated with the future should be avoided as much as possible, because;
     *
     * a) A result is already available. b) Some other required part of the computation failed, meaning that a result is
     * useless anyways. c) The request was cancelled.
     *
     * @return Boolean indiciating if this future is ready or not.
     */
    public boolean isReady();

    /**
     * Resolve the value of a future using a collection of futures.
     *
     * <pre>
     * List<Future<C>> - *using reducer* -> Future<T> (this)
     * </pre>
     *
     * The group will be connected to this future in that it's result will finish this future and any cancellations of
     * this future will cancel the entire group.
     *
     * <pre>
     * {@code
     *   List<Future<Integer>> futures = asyncListOperation();
     * 
     *   Future<Integer> future = ConcurrentFuture.newReduce(futures, Future.Reducer<Integer, Integer>() {
     *     Integer resolved(Collection<Integer> results, Collection<Exception> errors, Collection<CancelReason> cancelled) {
     *       return sum(results);
     *     }
     *   }
     * 
     *   # use future
     * }
     * </pre>
     *
     * @param futures
     *            Collection of futures to reduce.
     * @param reducer
     *            Function responsible for reducing the collection into a single object.
     * @return A new future with the generic value <T>.
     */
    public <C> Future<T> reduce(List<Future<C>> futures, final Reducer<C, T> reducer);

    public <C> Future<T> reduce(List<Future<C>> futures, final ErrorReducer<C, T> error);

    /**
     * Resolve the value of a future using a collection of futures. Similar to {@link #reduce(List, Reducer)} but using
     * the {@link StreamReducer} to receive results as they arrive. The benefit of a streamed approach is that
     * intermittent results do not have to be kept in memory.
     *
     * <pre>
     * {@code
     * List<Future<C>> - *using stream reducer* -> Future<T> (this)
     * }
     * </pre>
     *
     * The group will be connected to this future in that it's result will finish this future. Any cancellations of this
     * future will cancel the entire collection of futures.
     *
     * <pre>
     * {@code
     *   List<Future<Integer>> futures = asyncListOperation();
     * 
     *   Future<Integer> future = ConcurrentFuture.newReduce(futures, new Future.StreamReducer<Integer, Integer>() {
     *     final AtomicInteger value = new AtomicInteger(0);
     * 
     *     void finish(Future<Integer> future, Integer result) {
     *       value.addAndGet(result);
     *     }
     * 
     *     void failed(Future<Integer> future, Exception error) {
     *     }
     * 
     *     void cancel(Future<Integer> future, CancelReason reason) throws Exception {
     *     }
     * 
     *     Double resolved(int successful, int failed, int cancelled) throws Exception {
     *       return result.get();
     *     }
     *   });
     * 
     *   # use future
     * }
     * </pre>
     *
     * @param futures
     *            Collection of futures to reduce.
     * @param reducer
     *            Function responsible for reducing the collection into a single object.
     * @return A new future with the generic value <T>.
     */
    public <C> Future<T> reduce(List<Future<C>> futures, final StreamReducer<C, T> reducer);

    /**
     * Transforms the value of one future into another using a deferred transformer function.
     *
     * <pre>
     * Future<T> (this) - *using deferred transformer* -> Future<C>
     * </pre>
     *
     * A deferred transformer is expected to return a compatible future that when resolved will resolve the future that
     * this function returns.
     *
     * <pre>
     * {@code
     *   Future<Integer> first = asyncOperation();
     * 
     *   Future<Double> second = first.transform(new Transformer<Integer, Double>() {
     *     void transform(Integer result, Future<Double> future) {
     *       future.finish(result.doubleValue());
     *     }
     *   };
     * 
     *   # use second
     * }
     * </pre>
     *
     * @param transformer
     *            The function to use when transforming the value.
     * @return A future of type <C> which resolves with the transformed value.
     */
    public <C> Future<C> transform(DelayedTransform<T, C> transformer);

    /**
     * Transforms the value of this future into another type using a transformer function.
     *
     * <pre>
     * Future<T> (this) - *using transformer* -> Future<C>
     * </pre>
     *
     * Use this if the transformation performed does not require any more async operations.
     *
     * <pre>
     * {@code
     *   Future<Integer> first = asyncOperation();
     * 
     *   Future<Double> second = future.transform(new Transformer<Integer, Double>() {
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
    public <C> Future<C> transform(Transform<T, C> transformer);

    public <C> Future<C> transform(Transform<T, C> transformer, ErrorTransformer<C> error);

    /**
     * Block until result is available.
     *
     * @return The result of this future being resolved.
     * @throws Exception
     *             If the future being resolved threw an exception.
     */
    public T get() throws InterruptedException, CancelledException, FailedException;
}
