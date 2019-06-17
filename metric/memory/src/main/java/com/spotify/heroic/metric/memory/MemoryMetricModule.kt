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

package com.spotify.heroic.metric.memory

import com.fasterxml.jackson.annotation.JsonProperty
import com.spotify.heroic.common.DynamicModuleId
import com.spotify.heroic.common.Groups
import com.spotify.heroic.common.ModuleId
import com.spotify.heroic.dagger.PrimaryComponent
import com.spotify.heroic.metric.MetricModule
import dagger.Component
import dagger.Module
import dagger.Provides
import java.util.*

private val DEFAULT_GROUPS = Groups.of("memory")

@ModuleId("memory")
data class MemoryMetricModule(
    @JsonProperty("id") val id: Optional<String>,
    @JsonProperty("groups") var groups: Groups = DEFAULT_GROUPS,
    @JsonProperty("synchronizedStorage") val synchronizedStorage: Boolean = false
): MetricModule, DynamicModuleId {
    override fun module(
        primary: PrimaryComponent, depends: MetricModule.Depends, id: String
    ): MetricModule.Exposed {
        return DaggerMemoryMetricModule_C
            .builder()
            .primaryComponent(primary)
            .depends(depends)
            .m(M())
            .build()
    }

    override fun id(): Optional<String> = id

    @MemoryScope
    @Component(
        modules = [M::class],
        dependencies = [PrimaryComponent::class, MetricModule.Depends::class]
    )
    interface C: MetricModule.Exposed {
        override fun backend(): MemoryBackend
    }

    @Module
    inner class M {
        @Provides
        @MemoryScope
        fun groups() = groups
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }
}

data class Builder(
    var id: Optional<String> = Optional.empty(),
    var groups: Groups = DEFAULT_GROUPS,
    var synchronizedStorage: Boolean = false
) {
    fun id(id: String) = apply { this.id = Optional.of(id) }
    fun groups(groups: Groups) = apply { this.groups = groups }
    fun synchronizedStorage(synchronizedStorage: Boolean) =
        apply { this.synchronizedStorage = synchronizedStorage }
    fun build() = MemoryMetricModule(id, groups, synchronizedStorage)
}