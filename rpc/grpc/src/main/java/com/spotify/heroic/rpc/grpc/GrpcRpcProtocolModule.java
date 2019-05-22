/*
 * Copyright (c) 2015 Spotify AB.
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

package com.spotify.heroic.rpc.grpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.cluster.RpcProtocolComponent;
import com.spotify.heroic.cluster.RpcProtocolModule;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.ResolvableFuture;
import io.netty.channel.nio.NioEventLoopGroup;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.inject.Named;
import lombok.Data;

@Data
public class GrpcRpcProtocolModule implements RpcProtocolModule {
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 9698;
    private static final int DEFAULT_MAX_FRAME_SIZE = 10 * 1_000_000;

    private final InetSocketAddress address;
    private final int maxFrameSize;

    @JsonCreator
    public GrpcRpcProtocolModule(
        @JsonProperty("host") String host,
        @JsonProperty("port") Integer port,
        @JsonProperty("maxFrameSize") Integer maxFrameSize
    ) {
        this.address = new InetSocketAddress(Optional.ofNullable(host).orElse(DEFAULT_HOST),
            Optional.ofNullable(port).orElse(DEFAULT_PORT));
        this.maxFrameSize = Optional.ofNullable(maxFrameSize).orElse(DEFAULT_MAX_FRAME_SIZE);
    }

    @Override
    public RpcProtocolComponent module(final Dependencies dependencies) {
        return DaggerGrpcRpcProtocolModule_C
            .builder()
            .dependencies(dependencies)
            .m(new M())
            .build();
    }

    @GrpcRpcScope
    @Component(modules = M.class, dependencies = Dependencies.class)
    interface C extends RpcProtocolComponent {
        @Override
        GrpcRpcProtocol rpcProtocol();

        @Override
        LifeCycle life();
    }

    @Module
    class M {
        @Provides
        @GrpcRpcScope
        @Named("bindFuture")
        ResolvableFuture<InetSocketAddress> bindFuture(final AsyncFramework async) {
            return async.future();
        }

        @Provides
        @GrpcRpcScope
        @Named("grpcBindAddress")
        InetSocketAddress grpcBindAddress() {
            return address;
        }

        @Provides
        @GrpcRpcScope
        @Named("defaultPort")
        int defaultPort() {
            return DEFAULT_PORT;
        }

        @Provides
        @GrpcRpcScope
        @Named("maxFrameSize")
        int maxFrameSize() {
            return maxFrameSize;
        }

        @Provides
        @GrpcRpcScope
        @Named("boss")
        NioEventLoopGroup boss() {
            return new NioEventLoopGroup(4);
        }

        @Provides
        @GrpcRpcScope
        @Named("worker")
        NioEventLoopGroup worker() {
            return new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 4);
        }

        @Provides
        @GrpcRpcScope
        LifeCycle server(
            LifeCycleManager manager, GrpcRpcProtocolServer server
        ) {
            final List<LifeCycle> life = new ArrayList<>();
            life.add(manager.build(server));
            return LifeCycle.combined(life);
        }
    }

    @Override
    public String scheme() {
        return "grpc";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;
        private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;

        public Builder host(final String host) {
            this.host = host;
            return this;
        }

        public Builder port(final int port) {
            this.port = port;
            return this;
        }

        public Builder maxFrameSize(final int maxFrameSize) {
            this.maxFrameSize = maxFrameSize;
            return this;
        }

        public GrpcRpcProtocolModule build() {
            return new GrpcRpcProtocolModule(host, port, maxFrameSize);
        }
    }
}
