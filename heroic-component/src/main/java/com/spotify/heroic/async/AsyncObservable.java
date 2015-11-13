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

import java.util.Iterator;
import java.util.List;

import eu.toolchain.async.AsyncFuture;

public interface AsyncObservable<T> {
    void observe(AsyncObserver<T> observer) throws Exception;

    static <T> AsyncObservable<T> chain(final List<AsyncObservable<T>> observables) {
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

    static <T> AsyncObservable<T> empty() {
        return new AsyncObservable<T>() {
            @Override
            public void observe(final AsyncObserver<T> observer) throws Exception {
                observer.end();
            }
        };
    }
}
