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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.ResolvableFuture;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.internal.ServerImpl;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.Channel;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static io.grpc.stub.ServerCalls.asyncUnaryCall;

@Slf4j
public class GrpcRpcProtocolServer implements LifeCycles {
    private final AsyncFramework async;
    private final MetricManager metrics;
    private final MetadataManager metadata;
    private final SuggestManager suggest;
    private final NodeMetadata localMetadata;
    private final ObjectMapper mapper;
    private final ResolvableFuture<InetSocketAddress> bindFuture;
    private final InetSocketAddress address;
    private final int maxFrameSize;
    private final NioEventLoopGroup bossGroup;
    private final NioEventLoopGroup workerGroup;
    private final GrpcRpcContainer container;

    private final AtomicReference<Server> server = new AtomicReference<>();

    @Inject
    public GrpcRpcProtocolServer(
        AsyncFramework async, MetricManager metrics, MetadataManager metadata,
        SuggestManager suggest, NodeMetadata localMetadata,
        @Named("application/json+internal") ObjectMapper mapper,
        @Named("bindFuture") ResolvableFuture<InetSocketAddress> bindFuture,
        @Named("grpcBindAddress") InetSocketAddress address,
        @Named("maxFrameSize") int maxFrameSize, @Named("boss") NioEventLoopGroup bossGroup,
        @Named("worker") NioEventLoopGroup workerGroup
    ) {
        this.async = async;
        this.metrics = metrics;
        this.metadata = metadata;
        this.suggest = suggest;
        this.localMetadata = localMetadata;
        this.mapper = mapper;
        this.bindFuture = bindFuture;
        this.address = address;
        this.maxFrameSize = maxFrameSize;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.container = setupContainer();
    }

    private GrpcRpcContainer setupContainer() {
        final GrpcRpcContainer container = new GrpcRpcContainer();

        container.register(GrpcRpcProtocol.METADATA, empty -> async.resolved(localMetadata));

        container.register(GrpcRpcProtocol.METRICS_FULL_QUERY,
            g -> g.apply(metrics, MetricBackendGroup::query));

        container.register(GrpcRpcProtocol.METRICS_WRITE,
            g -> g.apply(metrics, MetricBackend::write));

        container.register(GrpcRpcProtocol.METADATA_FIND_TAGS,
            g -> g.apply(metadata, MetadataBackend::findTags));

        container.register(GrpcRpcProtocol.METADATA_FIND_KEYS,
            g -> g.apply(metadata, MetadataBackend::findKeys));

        container.register(GrpcRpcProtocol.METADATA_FIND_SERIES,
            g -> g.apply(metadata, MetadataBackend::findSeries));

        container.register(GrpcRpcProtocol.METADATA_COUNT_SERIES,
            g -> g.apply(metadata, MetadataBackend::countSeries));

        container.register(GrpcRpcProtocol.METADATA_WRITE,
            g -> g.apply(metadata, MetadataBackend::write));

        container.register(GrpcRpcProtocol.METADATA_DELETE_SERIES,
            g -> g.apply(metadata, MetadataBackend::deleteSeries));

        container.register(GrpcRpcProtocol.SUGGEST_TAG_KEY_COUNT,
            g -> g.apply(suggest, SuggestBackend::tagKeyCount));

        container.register(GrpcRpcProtocol.SUGGEST_TAG,
            g -> g.apply(suggest, SuggestBackend::tagSuggest));

        container.register(GrpcRpcProtocol.SUGGEST_KEY,
            g -> g.apply(suggest, SuggestBackend::keySuggest));

        container.register(GrpcRpcProtocol.SUGGEST_TAG_VALUES,
            g -> g.apply(suggest, SuggestBackend::tagValuesSuggest));

        container.register(GrpcRpcProtocol.SUGGEST_TAG_VALUE,
            g -> g.apply(suggest, SuggestBackend::tagValueSuggest));

        return container;
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    private AsyncFuture<Void> start() throws IOException {
        final Server server = NettyServerBuilder
            .forAddress(address)
            .addService(bindService())
            .maxMessageSize(maxFrameSize)
            .bossEventLoopGroup(bossGroup)
            .workerEventLoopGroup(workerGroup)
            .build();

        return async.call(() -> {
            server.start();
            this.server.set(server);
            return null;
        }).directTransform(v -> {
            final InetSocketAddress localAddress = extractInetSocketAddress(server);
            bindFuture.resolve(localAddress);
            return null;
        });
    }

    /**
     * Extract the local address from the current server.
     * <p>
     * Because no api is available to accomplish this, it currently uses a very ugly reflexive
     * approach.
     *
     * @param server Server to extract local address from.
     * @return an InetSocketAddress
     * @throws Exception if something goes wrong (which it should).
     */
    private InetSocketAddress extractInetSocketAddress(final Server server) throws Exception {
        final ServerImpl impl = (ServerImpl) server;

        final Field transportServerField = ServerImpl.class.getDeclaredField("transportServer");
        transportServerField.setAccessible(true);
        final Object transportServer = transportServerField.get(impl);

        final Field channelField = transportServer.getClass().getDeclaredField("channel");
        channelField.setAccessible(true);
        final Channel channel = (Channel) channelField.get(transportServer);

        return (InetSocketAddress) channel.localAddress();
    }

    private ServerServiceDefinition bindService() {
        final ServerServiceDefinition.Builder builder =
            ServerServiceDefinition.builder(GrpcRpcProtocol.SERVICE);

        for (final GrpcEndpointHandle<?, ?> spec : container.getEndpoints()) {
            final ServerCallHandler<byte[], byte[]> handler =
                serverCallHandlerFor((GrpcEndpointHandle<Object, Object>) spec);
            builder.addMethod(spec.descriptor(), handler);
        }

        return builder.build();
    }

    private ServerCallHandler<byte[], byte[]> serverCallHandlerFor(
        final GrpcEndpointHandle<Object, Object> spec
    ) {
        return asyncUnaryCall((request, observer) -> {
            final UUID id = UUID.randomUUID();

            log.trace("{}: Received request: {}", id, request);

            final AsyncFuture<Object> future;

            try {
                final Object obj = mapper.readValue(request, spec.queryType());
                future = spec.handle(obj);
            } catch (final Exception e) {
                log.error("{}: Failed to handle request (sent {})", id, Status.INTERNAL, e);
                observer.onError(new StatusException(Status.INTERNAL));
                return;
            }

            future.onDone(new FutureDone<Object>() {
                @Override
                public void failed(final Throwable cause) throws Exception {
                    log.error("{}: Request failed", id, cause);
                    observer.onError(cause);
                }

                @Override
                public void resolved(final Object result) throws Exception {
                    final byte[] body = mapper.writeValueAsBytes(result);
                    observer.onNext(body);
                    observer.onCompleted();
                }

                @Override
                public void cancelled() throws Exception {
                    observer.onError(new RuntimeException("Request cancelled"));
                }
            });
        });
    }

    private AsyncFuture<Void> stop() {
        final List<AsyncFuture<Void>> callbacks = new ArrayList<>();

        callbacks.add(async.call(() -> {
            server.get().shutdown();
            server.get().awaitTermination();
            return null;
        }));

        return async.collectAndDiscard(callbacks);
    }
}
