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

import com.spotify.heroic.instrumentation.OperationsLog;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Data;

@Data
public class FakeKafkaConnection implements KafkaConnection {
    private final OperationsLog opLog;
    private final Map<String, List<FakeKafkaStream<byte[]>>> streams = new HashMap<>();
    private final Random rand = new Random();

    @Override
    public Map<String, List<KafkaStream<byte[]>>> createMessageStreams(
        final Map<String, Integer> wantedStreams
    ) {
        final Map<String, List<FakeKafkaStream<byte[]>>> resultMap = new HashMap<>();
        for (Map.Entry<String, Integer> topicEntry : wantedStreams.entrySet()) {
            String topicName = topicEntry.getKey();
            List<FakeKafkaStream<byte[]>> streamList = new ArrayList<>();

            for (int i = 0; i < topicEntry.getValue(); i++) {
                streamList.add(new FakeKafkaStream<byte[]>());
            }

            resultMap.put(topicName, streamList);
        }

        streams.putAll(resultMap);

        final Map<String, ? extends List<? extends KafkaStream<byte[]>>> someKafkaStreamMap =
            resultMap;
        @SuppressWarnings("unchecked") final Map<String, List<KafkaStream<byte[]>>>
            unsafeCastToKafkaStreamMap =
            (Map<String, List<KafkaStream<byte[]>>>) (someKafkaStreamMap);
        return unsafeCastToKafkaStreamMap;
    }

    @Override
    public void commitOffsets() {
        opLog.registerConsumerOffsetsCommit();
    }

    @Override
    public void shutdown() {
        streams.entrySet().forEach(entry -> entry.getValue().forEach(FakeKafkaStream::shutdown));
    }

    public void publish(String topic, byte[] message) {
        List<FakeKafkaStream<byte[]>> topicStreams = streams.get(topic);
        if (topicStreams == null) {
            throw new RuntimeException("Topic not found: " + topic);
        }

        FakeKafkaStream<byte[]> stream = topicStreams.get(rand.nextInt(topicStreams.size()));

        stream.publish(message);
    }
}
