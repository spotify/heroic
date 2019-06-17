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

package com.spotify.heroic.rpc.jvm

import com.fasterxml.jackson.annotation.JsonProperty
import com.spotify.heroic.cluster.RpcProtocolComponent
import com.spotify.heroic.cluster.RpcProtocolModule
import com.spotify.heroic.lifecycle.LifeCycle
import com.spotify.heroic.lifecycle.LifeCycleManager
import dagger.Component
import dagger.Module
import dagger.Provides
import javax.inject.Named

data class JvmRpcProtocolModule(
    @JsonProperty("bindName") val bindName: String? = "heroic-jvm",
    val context: JvmRpcContext? = JvmRpcContext.globalContext()
) : RpcProtocolModule {

    override fun module(dependencies: RpcProtocolModule.Dependencies): RpcProtocolComponent {
        return DaggerJvmRpcProtocolModule_C
            .builder()
            .dependencies(dependencies)
            .m(M())
            .build()
    }

    override fun scheme(): String {
        return "jvm"
    }

    @JvmRpcScope
    @Component(modules = [M::class], dependencies = [RpcProtocolModule.Dependencies::class])
    interface C : RpcProtocolComponent {
        override fun rpcProtocol(): JvmRpcProtocol
        override fun life(): LifeCycle
    }

    @Module
    inner class M {
        @Provides
        @JvmRpcScope
        fun context() = context!!

        @Provides
        @JvmRpcScope
        @Named("bindName")
        fun bindName() = bindName!!

        @Provides
        @JvmRpcScope
        fun server(manager: LifeCycleManager, server: JvmRpcProtocolServer): LifeCycle {
            val life = arrayListOf<LifeCycle>(manager.build(server))
            return LifeCycle.combined(life)
        }
    }


    companion object {
        @JvmStatic
        fun builder(): Builder {
            return Builder()
        }
    }
}

data class Builder(
    var bindName: String? = null,
    var context: JvmRpcContext? = null
) {
    fun bindName(bindName: String) = apply { this.bindName = bindName }
    fun context(context: JvmRpcContext) = apply { this.context = context }
    fun build() = JvmRpcProtocolModule(bindName, context)
}