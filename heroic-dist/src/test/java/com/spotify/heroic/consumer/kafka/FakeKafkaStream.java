/*
 * Copyright (c) 2017 Spotify AB.
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

package com.spotify.heroic.consumer.kafka;

import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import lombok.Data;

@Data
public class FakeKafkaStream<T> implements KafkaStream<T> {
    private final LinkedBlockingQueue<Optional<T>> messages;
    private final Iterator iterator;
    private final Iterable<T> iterable;

    public FakeKafkaStream() {
        this.messages = new LinkedBlockingQueue<Optional<T>>();
        this.iterator = new Iterator();
        this.iterable = new Iterable<T>() {
            @Override
            public Iterator iterator() {
                return iterator;
            }
        };
    }

    void publish(T message) {
        messages.add(Optional.of(message));
    }

    void shutdown() {
        messages.add(Optional.empty());
    }

    @Override
    public Iterable<T> messageIterable() {
        return iterable;
    }

    @Data
    class Iterator implements java.util.Iterator<T> {

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public T next() {
            try {
                return messages.take().orElse(null);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
