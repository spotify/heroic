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

package com.spotify.heroic.rpc.grpc

import com.fasterxml.jackson.annotation.JsonProperty
import com.spotify.heroic.cluster.RpcProtocolComponent
import com.spotify.heroic.cluster.RpcProtocolModule
import com.spotify.heroic.lifecycle.LifeCycle
import com.spotify.heroic.lifecycle.LifeCycleManager
import dagger.Component
import dagger.Module
import dagger.Provides
import eu.toolchain.async.AsyncFramework
import io.netty.channel.nio.NioEventLoopGroup
import java.net.InetSocketAddress
import javax.inject.Named

private const val DEFAULT_HOST = "0.0.0.0"
private const val DEFAULT_PORT  = 9698
private const val DEFAULT_MAX_FRAME_SIZE = 10_000_000

data class GrpcRpcProtocolModule(
    @JsonProperty("host") val host: String?,
    @JsonProperty("port") val port: Int?,
    @JsonProperty("maxFrameSize") val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE
): RpcProtocolModule {
    val address: InetSocketAddress = InetSocketAddress(host ?: DEFAULT_HOST, port ?: DEFAULT_PORT)

    override fun module(dependencies: RpcProtocolModule.Dependencies): RpcProtocolComponent {
        return DaggerGrpcRpcProtocolModule_C
            .builder()
            .dependencies(dependencies)
            .m(M())
            .build()
    }

    override fun scheme(): String = "grpc"

    @GrpcRpcScope
    @Component(modules = [M::class], dependencies = [RpcProtocolModule.Dependencies::class])
    interface C: RpcProtocolComponent {
        override fun rpcProtocol(): GrpcRpcProtocol
        override fun life(): LifeCycle
    }

    @Module
    inner class M {
        @Provides
        @GrpcRpcScope
        @Named("bindFuture")
        fun bindFuture(async: AsyncFramework) = async.future<InetSocketAddress>()

        @Provides
        @GrpcRpcScope
        @Named("grpcBindAddress")
        fun grpcBindAddress() = address

        @Provides
        @GrpcRpcScope
        @Named("defaultPort")
        fun defaultPort() = DEFAULT_PORT

        @Provides
        @GrpcRpcScope
        @Named("maxFrameSize")
        fun maxFrameSize() = maxFrameSize

        @Provides
        @GrpcRpcScope
        @Named("boss")
        fun boss() = NioEventLoopGroup(4)

        @Provides
        @GrpcRpcScope
        @Named("worker")
        fun worker() = NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 4)

        @Provides
        @GrpcRpcScope
        fun server(manager: LifeCycleManager, server: GrpcRpcProtocolServer): LifeCycle {
            val life = arrayListOf<LifeCycle>(manager.build(server))
            return LifeCycle.combined(life)
        }
    }

    companion object {
        @JvmStatic
        fun builder() = Builder()
    }
}

data class Builder(
    var host: String = DEFAULT_HOST,
    var port: Int = DEFAULT_PORT,
    var maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE
) {
    fun host(host: String) = apply { this.host = host }
    fun port(port: Int) = apply { this.port = port }
    fun maxFrameSize(maxFrameSize: Int) = apply { this.maxFrameSize = maxFrameSize }

    fun build() = GrpcRpcProtocolModule(host, port, maxFrameSize)
}