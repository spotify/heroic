/*
 * Copyright (c) 2019 Spotify AB.
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

package com.spotify.heroic.shell.task.datamigrate

import com.spotify.heroic.async.AsyncObserver
import com.spotify.heroic.metric.BackendKey
import com.spotify.heroic.metric.MetricBackend
import com.spotify.heroic.metric.MetricCollection
import com.spotify.heroic.metric.WriteMetric
import eu.toolchain.async.AsyncFramework
import eu.toolchain.async.AsyncFuture
import eu.toolchain.async.ResolvableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.function.Consumer
import java.util.function.Supplier

internal class RowObserver(
    val errors: ConcurrentLinkedQueue<Throwable>,
    val to: MetricBackend,
    val future: ResolvableFuture<Void>,
    val key: BackendKey,
    val done: Supplier<Boolean>,
    val end: Consumer<BackendKey>,
    val async: AsyncFramework
) : AsyncObserver<MetricCollection> {

    // TODO: async can be injected with dagger once lombok is removed

    override fun observe(value: MetricCollection): AsyncFuture<Void> {
        if (future.isDone || done.get()) {
            return async.cancelled()
        }

        val write = to
            .write(WriteMetric.Request(key.series, value))
            .directTransform<Void> { null }

        future.bind(write)
        return write
    }

    override fun cancel() {
        end()
    }

    override fun fail(cause: Throwable) {
        errors.add(cause)
        end()
    }

    override fun end() {
        end.accept(key)
    }
}
