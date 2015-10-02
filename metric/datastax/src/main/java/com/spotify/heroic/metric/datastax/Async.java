package com.spotify.heroic.metric.datastax;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;

public final class Async {
    /**
     * Helper method to convert a {@link ListenableFuture} to an {@link AsyncFuture}.
     */
    public static <T> AsyncFuture<T> bind(AsyncFramework async, ListenableFuture<T> source) {
        final ResolvableFuture<T> target = async.future();

        Futures.addCallback(source, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                target.resolve(result);
            }

            @Override
            public void onFailure(Throwable t) {
                target.fail(t);
            }
        });

        target.onCancelled(() -> {
            source.cancel(false);
        });

        return target;
    }
}