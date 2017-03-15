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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import kafka.javaapi.consumer.ConsumerConnector;
import lombok.Data;

@Data
public class RealKafkaConnection implements KafkaConnection {
    private final ConsumerConnector connector;

    @Override
    public Map<String, List<KafkaStream<byte[]>>> createMessageStreams(
        final Map<String, Integer> wantedStreams
    ) {
        Set<Map.Entry<String, List<kafka.consumer.KafkaStream<byte[], byte[]>>>> realStreams =
            connector.createMessageStreams(wantedStreams).entrySet();

        Map<String, List<KafkaStream<byte[]>>> resultMap = new HashMap<>();
        for (Map.Entry<String, List<kafka.consumer.KafkaStream<byte[], byte[]>>> e : realStreams) {
            List<KafkaStream<byte[]>> streamList = new ArrayList<>();

            for (kafka.consumer.KafkaStream<byte[], byte[]> s : e.getValue()) {
                streamList.add(new RealKafkaStream<>(s));
            }

            resultMap.put(e.getKey(), streamList);
        }

        return resultMap;
    }

    @Override
    public void commitOffsets() {
        connector.commitOffsets();
    }

    @Override
    public void shutdown() {
        connector.shutdown();
    }
}
