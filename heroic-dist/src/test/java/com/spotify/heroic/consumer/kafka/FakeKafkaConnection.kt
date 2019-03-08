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

import com.spotify.heroic.instrumentation.OperationsLog
import java.util.*

data class FakeKafkaConnection(val opLog: OperationsLog) : KafkaConnection {
    private val streams = HashMap<String, List<FakeKafkaStream<ByteArray>>>()
    private val rand = Random()

    override fun createMessageStreams(
        wantedStreams: Map<String, Int>
    ): Map<String, List<KafkaStream<ByteArray>>> {
        val resultMap = HashMap<String, List<FakeKafkaStream<ByteArray>>>()
        for ((topicName, value) in wantedStreams) {
            val streamList = ArrayList<FakeKafkaStream<ByteArray>>()

            for (i in 0 until value) {
                streamList.add(FakeKafkaStream())
            }

            resultMap[topicName] = streamList
        }

        streams.putAll(resultMap)

        return resultMap
    }

    override fun commitOffsets() {
        opLog.registerConsumerOffsetsCommit()
    }

    override fun shutdown() {
        streams.entries.forEach { entry -> entry.value.forEach { it.shutdown() } }
    }

    fun publish(topic: String, message: ByteArray) {
        val topicStreams = streams[topic] ?: throw RuntimeException("Topic not found: $topic")

        val stream = topicStreams[rand.nextInt(topicStreams.size)]

        stream.publish(message)
    }
}
