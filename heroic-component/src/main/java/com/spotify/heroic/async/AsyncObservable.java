/*
 * Copyright (c) 2015 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.heroic.async;

import com.spotify.heroic.common.Throwing;
import eu.toolchain.async.AsyncFuture;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public interface AsyncObservable<T> {
    void observe(AsyncObserver<T> observer);

    /**
     * Transform this observable to another type.
     *
     * @param transform The transformation to perform.
     * @return A new Observable of the transformed type.
     */
    default <S> AsyncObservable<S> transform(final Function<T, S> transform) {
        return observer -> {
            observe(new AsyncObserver<T>() {
                @Override
                public AsyncFuture<Void> observe(T value) {
                    return observer.observe(transform.apply(value));
                }

                @Override
                public void cancel() {
                    observer.cancel();
                }

                @Override
                public void fail(Throwable cause) {
                    observer.fail(cause);
                }

                @Override
                public void end() {
                    observer.end();
                }
            });
        };
    }

    default AsyncObservable<T> onFinished(final Runnable end) {
        return observer -> {
            observe(new AsyncObserver<T>() {
                @Override
                public AsyncFuture<Void> observe(T value) {
                    return observer.observe(value);
                }

                @Override
                public void cancel() {
                    Throwing.call(observer::cancel, end);
                }

                @Override
                public void fail(Throwable cause) {
                    Throwing.call(() -> observer.fail(cause), end);
                }

                @Override
                public void end() {
                    Throwing.call(observer::end, end);
                }
            });
        };
    }

    static <T> AsyncObservable<T> chain(final List<AsyncObservable<T>> observables) {
        return new AsyncObservable<T>() {
            @Override
            public void observe(final AsyncObserver<T> observer) {
                final Iterator<AsyncObservable<T>> it = observables.iterator();

                if (!it.hasNext()) {
                    observer.end();
                    return;
                }

                final AsyncObserver<T> chainer = new AsyncObserver<T>() {
                    @Override
                    public AsyncFuture<Void> observe(T value) {
                        return observer.observe(value);
                    }

                    @Override
                    public void cancel() {
                        observer.cancel();
                    }

                    @Override
                    public void fail(Throwable cause) {
                        observer.fail(cause);
                    }

                    @Override
                    public void end() {
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

    static <T> AsyncObservable<T> empty() {
        return AsyncObserver::end;
    }

    /**
     * Create an observable that will always be immediately failed with the given throwable.
     */
    static <T> AsyncObservable<T> failed(final Throwable e) {
        return observer -> observer.fail(e);
    }
}
