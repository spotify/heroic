package com.spotify.heroic.async;

import java.util.Iterator;
import java.util.List;

import eu.toolchain.async.AsyncFuture;

public interface AsyncObservable<T> {
    public void observe(AsyncObserver<T> observer) throws Exception;

    public static <T> AsyncObservable<T> chain(final List<AsyncObservable<T>> observables) {
        return new AsyncObservable<T>() {
            @Override
            public void observe(final AsyncObserver<T> observer) throws Exception {
                final Iterator<AsyncObservable<T>> it = observables.iterator();

                if (!it.hasNext()) {
                    observer.end();
                    return;
                }

                final AsyncObserver<T> chainer = new AsyncObserver<T>() {
                    @Override
                    public AsyncFuture<Void> observe(T value) throws Exception {
                        return observer.observe(value);
                    }

                    @Override
                    public void cancel() throws Exception {
                        observer.cancel();
                    }

                    @Override
                    public void fail(Throwable cause) throws Exception {
                        observer.fail(cause);
                    }

                    @Override
                    public void end() throws Exception {
                        if (!it.hasNext()) {
                            observer.end();
                            return;
                        }

                        it.next().observe(this);
                    }
                };

                it.next().observe(chainer);
            }
        };
    }
}