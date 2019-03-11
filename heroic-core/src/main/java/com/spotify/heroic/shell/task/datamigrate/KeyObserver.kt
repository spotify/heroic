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

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import com.spotify.heroic.async.AsyncObserver
import com.spotify.heroic.filter.Filter
import com.spotify.heroic.metric.BackendKey
import com.spotify.heroic.metric.BackendKeySet
import com.spotify.heroic.metric.MetricBackend
import com.spotify.heroic.shell.ShellIO
import com.spotify.heroic.shell.task.parameters.DataMigrateParameters
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings
import eu.toolchain.async.AsyncFramework
import eu.toolchain.async.AsyncFuture
import eu.toolchain.async.ResolvableFuture
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer
import java.util.function.Supplier

internal class KeyObserver(
    val io: ShellIO,
    val params: DataMigrateParameters,
    val filter: Filter,
    val from: MetricBackend,
    val to: MetricBackend,
    val future: ResolvableFuture<Void>,
    val errors: ConcurrentLinkedQueue<Throwable>,
    val async: AsyncFramework,
    val mapper: ObjectMapper
) : AsyncObserver<BackendKeySet> {

    // TODO: async and mapper can be injected with dagger once lombok is removed

    val lock = Any()

    /**
     * must synchronize access with [.lock]
     */
    @Volatile
    var done = false

    var pending = 0
    var next: ResolvableFuture<Void>? = null

    /* a queue of the next keys to migrate */
    val current = ConcurrentLinkedQueue<BackendKey>()

    /* the total number of keys migrated */
    val total = AtomicLong()

    /* the total number of failed keys */
    val failedKeys = AtomicLong()

    /* the total number of keys */
    val totalKeys = AtomicLong()

    override fun observe(set: BackendKeySet): AsyncFuture<Void>? {
        if (next != null) {
            return async.failed(RuntimeException("next future is still set"))
        }

        failedKeys.addAndGet(set.failedKeys)
        totalKeys.addAndGet(set.failedKeys + set.keys.size)

        if (errors.size > DataMigrate.ALLOWED_ERRORS) {
            return async.failed(RuntimeException("too many failed migrations"))
        }

        if (failedKeys.get() > DataMigrate.ALLOWED_FAILED_KEYS) {
            return async.failed(RuntimeException("too many failed keys"))
        }

        if (future.isDone) {
            return async.cancelled()
        }

        if (set.keys.isEmpty()) {
            return async.resolved()
        }

        current.addAll(set.keys)

        synchronized(lock) {
            next = async.future()

            while (true) {
                if (pending >= params.parallelism) {
                    break
                }

                val k = current.poll() ?: break

                pending++
                streamOne(k)
            }

            return if (pending < params.parallelism) {
                async.resolved()
            } else next
        }
    }

    private fun streamOne(key: BackendKey) {
        if (!filter.apply(key.series)) {
            endOne(key)
            return
        }

        val observer = RowObserver(errors, to, future, key,
            Supplier { done }, Consumer { endOneRuntime(it) }, async)
        from.streamRow(key).observe(observer)
    }

    private fun endOneRuntime(key: BackendKey) {
        try {
            endOne(key)
        } catch (e: Exception) {
            throw RuntimeException(e)
        }

    }

    @SuppressFBWarnings(value = ["RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"])
    private fun endOne(key: BackendKey) {
        streamDot(io, key, total.incrementAndGet())

        // opportunistically pick up the next available task without locking (if available).
        val k: BackendKey? = current.poll()

        if (k != null) {
            streamOne(k)
            return
        }

        synchronized(lock) {
            pending--

            if (next != null) {
                val tmp = next!!
                next = null
                tmp.resolve(null)
            }

            checkFinished()
        }
    }

    override fun cancel() {
        synchronized(io) {
            io.out().println("Cancelled when reading keys")
        }

        end()
    }

    override fun fail(cause: Throwable) {
        synchronized(io) {
            io.out().println("Error when reading keys: " + cause.message)
            cause.printStackTrace(io.out())
            io.out().flush()
        }

        end()
    }

    override fun end() {
        synchronized(lock) {
            done = true
            checkFinished()
        }
    }

    fun checkFinished() {
        if (done && pending == 0) {
            future.resolve(null)
        }
    }

    fun streamDot(io: ShellIO, key: BackendKey, n: Long) {
        if (n % DataMigrate.LINES == 0L) {
            synchronized(io) {
                try {
                    io.out().println(" failedKeys: " + failedKeys.get() + ", last: " +
                        mapper.writeValueAsString(key))
                } catch (e: JsonProcessingException) {
                    throw RuntimeException(e)
                }

                io.out().flush()
            }
        } else if (n % DataMigrate.DOTS == 0L) {
            synchronized(io) {
                io.out().print(".")
                io.out().flush()
            }
        }
    }
}
