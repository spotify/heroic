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

import java.util.Iterator;

public class RealKafkaStream<V> implements KafkaStream<V> {
    private final kafka.consumer.KafkaStream<byte[], V> stream;
    private final KafkaIterable kafkaIterable;
    private final KafkaIterator kafkaIterator;

    public RealKafkaStream(final kafka.consumer.KafkaStream<byte[], V> stream) {
        this.stream = stream;
        this.kafkaIterator = new KafkaIterator();
        this.kafkaIterable = new KafkaIterable();
    }

    @Override
    public Iterable<V> messageIterable() {
        return kafkaIterable;
    }

    class KafkaIterable implements Iterable<V> {
        @Override
        public Iterator<V> iterator() {
            return kafkaIterator;
        }
    }

    class KafkaIterator implements Iterator<V> {

        @Override
        public boolean hasNext() {
            return stream.iterator().hasNext();
        }

        @Override
        public V next() {
            return stream.iterator().next().message();
        }
    }
}
