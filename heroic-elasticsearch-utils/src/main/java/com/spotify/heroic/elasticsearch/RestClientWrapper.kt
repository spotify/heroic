/*
 * Copyright (c) 2020 Spotify AB.
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

package com.spotify.heroic.elasticsearch

import com.spotify.heroic.elasticsearch.index.IndexMapping
import eu.toolchain.async.AsyncFramework
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import java.net.InetAddress
import java.net.UnknownHostException

private const val DEFAULT_PORT = 9200

data class RestClientWrapper(
    val seeds: List<String> = listOf("localhost")
): ClientWrapper {
    val client = RestHighLevelClient(RestClient.builder(*parseSeeds()))

    override fun start(
        async: AsyncFramework,
        index: IndexMapping,
        templateName: String,
        type: BackendType
    ): Connection {
        return RestConnection(client, async, index, templateName, type)
    }

    private fun parseSeeds(): Array<HttpHost> {
        return seeds
            .map(::parseInetSocketTransportAddress)
            .map {(host, port) -> HttpHost(host, port) }
            .toTypedArray()
    }

    private fun parseInetSocketTransportAddress(seed: String): Pair<InetAddress, Int> {
        try {
            if (seed.contains(":")) {
                val parts = seed.split(":")
                return InetAddress.getByName(parts.first()) to parts.last().toInt()
            }

            return InetAddress.getByName(seed) to DEFAULT_PORT
        } catch (e: UnknownHostException) {
            throw RuntimeException(e)
        }
    }
}