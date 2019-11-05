/*
 * Copyright (c) 2019 Spotify AB.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"): you may not use this file except in compliance
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

package com.spotify.heroic.metadata.elasticsearch.actor

import akka.actor.Props
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.hash.HashCode
import com.spotify.heroic.common.DynamicModuleId
import com.spotify.heroic.elasticsearch.ConnectionModule
import com.spotify.heroic.elasticsearch.RateLimitedCache
import com.spotify.heroic.metadata.MetadataActorModule
import com.spotify.heroic.statistics.HeroicReporter
import com.spotify.heroic.statistics.MetadataBackendReporter
import java.util.*

class ElasticsearchMetadataActorModule(
    val id: String?,
    @JsonProperty("connection") val connectionModule: ConnectionModule = ConnectionModule.buildDefault(),
    val deleteParallelism: Int = 20,
    val templateName: String = "heroic-metadata",
    backendType: String?,
    val configure: Boolean = false
): MetadataActorModule, DynamicModuleId {
    override lateinit var reporter: HeroicReporter
    //lateinit var writeCache: RateLimitedCache<Pair<String, HashCode>>

    companion object {
        private val defaultSetup = ElasticsearchMetadataActor.Companion::backendType
        private val backendTypes = mapOf("kv" to defaultSetup)

        @JvmStatic
        fun types() = backendTypes.keys.toList()
    }

    private val backendType = backendTypes.getOrDefault(backendType, defaultSetup)()

    override val name = "elasticsearch"

    override fun id(): Optional<String> = Optional.ofNullable(id)

    override fun props(): Props {
        val connection: Props = connectionModule.connectionActor(templateName, backendType)
        return Props.create(ElasticsearchMetadataActor::class.java,
            reporter, connection, configure, deleteParallelism)
    }
}
