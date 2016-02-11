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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.RpcProtocolComponent;
import com.spotify.heroic.cluster.RpcProtocolModule;
import com.spotify.heroic.dagger.PrimaryComponent;
import com.spotify.heroic.lifecycle.LifeCycle;
import com.spotify.heroic.lifecycle.LifeCycleManager;
import com.spotify.heroic.metadata.MetadataComponent;
import com.spotify.heroic.metric.MetricComponent;
import com.spotify.heroic.suggest.SuggestComponent;
import dagger.Component;
import dagger.Module;
import dagger.Provides;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.ResolvableFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import lombok.Data;
import lombok.RequiredArgsConstructor;

import javax.inject.Named;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Optional;

@Data
public class NativeRpcProtocolModule implements RpcProtocolModule {
    private static final String DEFAULT_HOST = "0.0.0.0";
    private static final int DEFAULT_PORT = 0;
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
    private final NativeEncoding encoding;

    @JsonCreator
    public NativeRpcProtocolModule(
        @JsonProperty("host") String host, @JsonProperty("port") Integer port,
        @JsonProperty("parentThreads") Integer parentThreads,
        @JsonProperty("childThreads") Integer childThreads,
        @JsonProperty("maxFrameSize") Integer maxFrameSize,
        @JsonProperty("heartbeatInterval") Long heartbeatInterval,
        @JsonProperty("sendTimeout") Long sendTimeout,
        @JsonProperty("encoding") Optional<NativeEncoding> encoding
    ) {
        this.address = new InetSocketAddress(Optional.ofNullable(host).orElse(DEFAULT_HOST),
            Optional.ofNullable(port).orElse(DEFAULT_PORT));
        this.parentThreads = Optional.ofNullable(parentThreads).orElse(DEFAULT_PARENT_THREADS);
        this.childThreads = Optional.ofNullable(childThreads).orElse(DEFAULT_CHILD_THREADS);
        this.maxFrameSize = Optional.ofNullable(maxFrameSize).orElse(DEFAULT_MAX_FRAME_SIZE);
        this.heartbeatInterval =
            Optional.ofNullable(heartbeatInterval).orElse(DEFAULT_HEARTBEAT_INTERVAL);
        this.sendTimeout = Optional.ofNullable(sendTimeout).orElse(DEFAULT_SEND_TIMEOUT);
        this.encoding = encoding.orElse(NativeEncoding.GZIP);
    }

    @Override
    public RpcProtocolComponent module(
        PrimaryComponent primary, MetricComponent metric, MetadataComponent metadata,
        SuggestComponent suggest, NodeMetadata nodeMetadata
    ) {
        final C c = DaggerNativeRpcProtocolModule_C
            .builder()
            .primaryComponent(primary)
            .metricComponent(metric)
            .metadataComponent(metadata)
            .suggestComponent(suggest)
            .m(new M(nodeMetadata))
            .build();

        return c;
    }

    @NativeRpcScope
    @Component(modules = M.class,
        dependencies = {
            PrimaryComponent.class, SuggestComponent.class, MetricComponent.class,
            MetadataComponent.class
        })
    interface C extends RpcProtocolComponent {
        @Override
        NativeRpcProtocol rpcProtocol();

        @Override
        LifeCycle life();
    }

    @RequiredArgsConstructor
    @Module
    class M {
        private final NodeMetadata nodeMetadata;

        @Provides
        @NativeRpcScope
        NodeMetadata nodeMetadata() {
            return nodeMetadata;
        }

        @Provides
        @NativeRpcScope
        @Named("bindFuture")
        public ResolvableFuture<InetSocketAddress> bindFuture(final AsyncFramework async) {
            return async.future();
        }

        @Provides
        @NativeRpcScope
        @Named("boss")
        public EventLoopGroup bossGroup() {
            return new NioEventLoopGroup(parentThreads);
        }

        @Provides
        @NativeRpcScope
        @Named("worker")
        public EventLoopGroup workerGroup() {
            return new NioEventLoopGroup(childThreads);
        }

        @Provides
        @NativeRpcScope
        public Timer timer() {
            return new HashedWheelTimer(
                new ThreadFactoryBuilder().setNameFormat("nativerpc-timer-%d").build());
        }

        @Provides
        @NativeRpcScope
        public NativeEncoding encoding() {
            return encoding;
        }

        @Provides
        @NativeRpcScope
        @Named("bindAddress")
        SocketAddress bindAddress() {
            return address;
        }

        @Provides
        @NativeRpcScope
        @Named("defaultPort")
        int defaultPort() {
            return DEFAULT_PORT;
        }

        @Provides
        @NativeRpcScope
        @Named("maxFrameSize")
        int maxFrameSize() {
            return maxFrameSize;
        }

        @Provides
        @NativeRpcScope
        @Named("sendTimeout")
        long sendTimeout() {
            return sendTimeout;
        }

        @Provides
        @NativeRpcScope
        @Named("heartbeatReadInterval")
        long heartbeatReadInterval() {
            return heartbeatInterval;
        }

        @Provides
        @NativeRpcScope
        LifeCycle server(LifeCycleManager manager, NativeRpcProtocolServer server) {
            return manager.build(server);
        }
    }

    @Override
    public String scheme() {
        return "nativerpc";
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String host = DEFAULT_HOST;
        private int port = DEFAULT_PORT;
        private int parentThreads = DEFAULT_PARENT_THREADS;
        private int childThreads = DEFAULT_CHILD_THREADS;
        private int maxFrameSize = DEFAULT_MAX_FRAME_SIZE;
        private long sendTimeout = DEFAULT_SEND_TIMEOUT;
        private long heartbeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
        private NativeEncoding encoding = NativeEncoding.GZIP;

        public Builder host(final String host) {
            this.host = host;
            return this;
        }

        public Builder port(final int port) {
            this.port = port;
            return this;
        }

        public Builder parentThreads(final int parentThreads) {
            this.parentThreads = parentThreads;
            return this;
        }

        public Builder childThreads(final int childThreads) {
            this.childThreads = childThreads;
            return this;
        }

        public Builder maxFrameSize(final int maxFrameSize) {
            this.maxFrameSize = maxFrameSize;
            return this;
        }

        public Builder sendTimeout(final long sendtimeout) {
            this.sendTimeout = sendtimeout;
            return this;
        }

        public Builder heartbeatInterval(final int heartbeatInterval) {
            this.heartbeatInterval = heartbeatInterval;
            return this;
        }

        public Builder encoding(final NativeEncoding encoding) {
            this.encoding = encoding;
            return this;
        }

        public NativeRpcProtocolModule build() {
            return new NativeRpcProtocolModule(host, port, parentThreads, childThreads,
                maxFrameSize, sendTimeout, heartbeatInterval, Optional.of(encoding));
        }
    }
}
