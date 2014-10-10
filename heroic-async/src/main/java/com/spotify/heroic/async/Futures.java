package com.spotify.heroic.async;

import java.util.List;
import java.util.concurrent.Executor;

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

    public static <T> Future<T> resolved(T value) {
        return new ResolvedFuture<T>(value);
    }

    public static <T> Future<T> failed(Exception e) {
        return new FailedFuture<T>(e);
    }

    public static <T> Future<T> cancelled(CancelReason reason) {
        return new CancelledFuture<T>(reason);
    }
}
