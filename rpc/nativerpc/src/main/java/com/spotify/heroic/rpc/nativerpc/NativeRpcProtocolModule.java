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

package com.spotify.heroic.rpc.nativerpc;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;

import java.net.InetSocketAddress;

import lombok.RequiredArgsConstructor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.spotify.heroic.cluster.RpcProtocol;
import com.spotify.heroic.cluster.RpcProtocolModule;

@RequiredArgsConstructor
public class NativeRpcProtocolModule implements RpcProtocolModule {
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 1394;
    private static final int DEFAULT_PARENT_THREADS = 2;
    private static final int DEFAULT_CHILD_THREADS = 100;
    private static final int DEFAULT_MAX_FRAME_SIZE = 10 * 1000000;
    private static final long DEFAULT_SEND_TIMEOUT = 5000;
    private static final long DEFAULT_HEARTBEAT_INTERVAL = 1000;

    private final InetSocketAddress address;
    private final int parentThreads;
    private final int childThreads;
    private final int maxFrameSize;
    private final long sendTimeout;
    private final long heartbeatInterval;

    @JsonCreator
    public NativeRpcProtocolModule(@JsonProperty("host") String host, @JsonProperty("port") Integer port,
            @JsonProperty("parentThreads") Integer parentThreads, @JsonProperty("childThreads") Integer childThreads,
            @JsonProperty("maxFrameSize") Integer maxFrameSize,
            @JsonProperty("heartbeatInterval") Long heartbeatInterval, @JsonProperty("sendTimeout") Long sendTimeout) {
        this.address = new InetSocketAddress(Optional.fromNullable(host).or(DEFAULT_HOST), Optional.fromNullable(port)
                .or(DEFAULT_PORT));
        this.parentThreads = Optional.fromNullable(parentThreads).or(DEFAULT_PARENT_THREADS);
        this.childThreads = Optional.fromNullable(childThreads).or(DEFAULT_CHILD_THREADS);
        this.maxFrameSize = Optional.fromNullable(maxFrameSize).or(DEFAULT_MAX_FRAME_SIZE);
        this.heartbeatInterval = Optional.fromNullable(heartbeatInterval).or(DEFAULT_HEARTBEAT_INTERVAL);
        this.sendTimeout = Optional.fromNullable(sendTimeout).or(DEFAULT_SEND_TIMEOUT);
    }

    @Override
    public Module module(final Key<RpcProtocol> key) {
        return new PrivateModule() {
            @Provides
            @Singleton
            @Named("boss")
            public EventLoopGroup bossGroup() {
                return new NioEventLoopGroup(parentThreads);
            }

            @Provides
            @Singleton
            @Named("worker")
            public EventLoopGroup workerGroup() {
                return new NioEventLoopGroup(childThreads);
            }

            @Provides
            @Singleton
            public Timer timer() {
                return new HashedWheelTimer(new ThreadFactoryBuilder().setNameFormat("nativerpc-timer-%d").build());
            }

            @Override
            protected void configure() {
                final long heartbeatReadInterval = heartbeatInterval * 2;

                bind(key).toInstance(
                        new NativeRpcProtocol(address, DEFAULT_PORT, maxFrameSize, sendTimeout, heartbeatInterval,
                                heartbeatReadInterval));

                expose(key);
            }
        };
    }

    @Override
    public String scheme() {
        return "nativerpc";
    }
}