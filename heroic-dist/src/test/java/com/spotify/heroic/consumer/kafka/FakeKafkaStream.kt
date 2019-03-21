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

package com.spotify.heroic.consumer.kafka

import java.util.*
import java.util.concurrent.LinkedBlockingQueue

data class FakeKafkaStream<T>(
    val messages: LinkedBlockingQueue<Optional<T>> = LinkedBlockingQueue()
) : KafkaStream<T> {
    private val iterator = KafkaIterator()
    private val iterable = object : Iterable<T> {
        override fun iterator(): Iterator<T> {
            return iterator
        }
    }

    fun publish(message: T) {
        messages.add(Optional.of(message))
    }

    fun shutdown() {
        messages.add(Optional.empty())
    }

    override fun messageIterable(): Iterable<T> {
        return iterable
    }

    inner class KafkaIterator : Iterator<T> {
        override fun hasNext(): Boolean {
            return true
        }

        override fun next(): T {
            try {
                return messages.take().orElse(null)
            } catch (e: InterruptedException) {
                throw RuntimeException(e)
            }
        }
    }
}


