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
