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

import com.spotify.heroic.common.Duration
import com.spotify.heroic.elasticsearch.index.IndexMapping
import eu.toolchain.async.AsyncFramework
import org.apache.http.HttpHost
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.client.sniff.Sniffer
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms
import java.net.InetAddress
import java.net.UnknownHostException
import java.util.concurrent.TimeUnit

private const val DEFAULT_PORT = 9200

data class RestClientWrapper @JvmOverloads constructor(
    val seeds: List<String> = listOf("localhost"),
    val sniff: Boolean = false,
    val sniffInterval: Duration = Duration.of(30, TimeUnit.SECONDS)
): ClientWrapper<ParsedStringTerms> {
    private val client = RestHighLevelClient(RestClient.builder(*parseSeeds()))

    private val sniffer: Sniffer? = if (sniff) {
        Sniffer.builder(client.lowLevelClient)
            .setSniffIntervalMillis(sniffInterval.toMilliseconds().toInt())
            .build()
    } else {
        null
    }

    override fun start(
        async: AsyncFramework,
        index: IndexMapping,
        templateName: String,
        type: BackendType
    ): Connection<ParsedStringTerms> {
        return RestConnection(client, sniffer, async, index, templateName, type)
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