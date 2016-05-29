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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.lifecycle.LifeCycleRegistry;
import com.spotify.heroic.lifecycle.LifeCycles;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.MetricBackend;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.rpc.nativerpc.NativeRpcProtocol.GroupedQuery;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcEmptyBody;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.Timer;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

public class NativeRpcProtocolServer implements LifeCycles {
    private final AsyncFramework async;
    private final MetricManager metrics;
    private final MetadataManager metadata;
    private final SuggestManager suggest;
    private final NodeMetadata localMetadata;
    private final ObjectMapper mapper;
    private final Timer timer;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final NativeEncoding encoding;
    private final ResolvableFuture<InetSocketAddress> bindFuture;

    private final SocketAddress address;
    private final int maxFrameSize;

    private final AtomicReference<Channel> serverChannel = new AtomicReference<>();
    private final NativeRpcContainer container = new NativeRpcContainer();

    @Inject
    public NativeRpcProtocolServer(
        AsyncFramework async, MetricManager metrics, MetadataManager metadata,
        SuggestManager suggest, NodeMetadata localMetadata,
        @Named("application/json+internal") ObjectMapper mapper, Timer timer,
        @Named("boss") EventLoopGroup bossGroup, @Named("worker") EventLoopGroup workerGroup,
        NativeEncoding encoding,
        @Named("bindFuture") ResolvableFuture<InetSocketAddress> bindFuture,
        @Named("bindAddress") SocketAddress address, @Named("maxFrameSize") int maxFrameSize
    ) {
        this.async = async;
        this.metrics = metrics;
        this.metadata = metadata;
        this.suggest = suggest;
        this.localMetadata = localMetadata;
        this.mapper = mapper;
        this.timer = timer;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.encoding = encoding;
        this.bindFuture = bindFuture;
        this.address = address;
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    public void register(LifeCycleRegistry registry) {
        registry.start(this::start);
        registry.stop(this::stop);
    }

    {
        container.register("metadata", new NativeRpcEndpoint<NativeRpcEmptyBody, NodeMetadata>() {
            @Override
            public AsyncFuture<NodeMetadata> handle(final NativeRpcEmptyBody request)
                throws Exception {
                return async.resolved(localMetadata);
            }
        });

        container.register(NativeRpcProtocol.METRICS_QUERY,
            new NativeRpcEndpoint<GroupedQuery<FullQuery.Request>, FullQuery>() {
                @Override
                public AsyncFuture<FullQuery> handle(final GroupedQuery<FullQuery.Request> g)
                    throws Exception {
                    return g.apply(metrics, MetricBackendGroup::query);
                }
            });

        container.register(NativeRpcProtocol.METRICS_WRITE,
            new NativeRpcEndpoint<GroupedQuery<WriteMetric.Request>, WriteMetric>() {
                @Override
                public AsyncFuture<WriteMetric> handle(final GroupedQuery<WriteMetric.Request> g)
                    throws Exception {
                    return g.apply(metrics, MetricBackend::write);
                }
            });

        container.register(NativeRpcProtocol.METADATA_FIND_TAGS,
            new NativeRpcEndpoint<GroupedQuery<FindTags.Request>, FindTags>() {
                @Override
                public AsyncFuture<FindTags> handle(final GroupedQuery<FindTags.Request> g)
                    throws Exception {
                    return g.apply(metadata, MetadataBackend::findTags);
                }
            });

        container.register(NativeRpcProtocol.METADATA_FIND_KEYS,
            new NativeRpcEndpoint<GroupedQuery<FindKeys.Request>, FindKeys>() {
                @Override
                public AsyncFuture<FindKeys> handle(final GroupedQuery<FindKeys.Request> g)
                    throws Exception {
                    return g.apply(metadata, MetadataBackend::findKeys);
                }
            });

        container.register(NativeRpcProtocol.METADATA_FIND_SERIES,
            new NativeRpcEndpoint<GroupedQuery<FindSeries.Request>, FindSeries>() {
                @Override
                public AsyncFuture<FindSeries> handle(final GroupedQuery<FindSeries.Request> g)
                    throws Exception {
                    return g.apply(metadata, MetadataBackend::findSeries);
                }
            });

        container.register(NativeRpcProtocol.METADATA_COUNT_SERIES,
            new NativeRpcEndpoint<GroupedQuery<CountSeries.Request>, CountSeries>() {
                @Override
                public AsyncFuture<CountSeries> handle(final GroupedQuery<CountSeries.Request> g)
                    throws Exception {
                    return g.apply(metadata, MetadataBackend::countSeries);
                }
            });

        container.register(NativeRpcProtocol.METADATA_WRITE,
            new NativeRpcEndpoint<GroupedQuery<WriteMetadata.Request>, WriteMetadata>() {
                @Override
                public AsyncFuture<WriteMetadata> handle(
                    final GroupedQuery<WriteMetadata.Request> g
                ) throws Exception {
                    return g.apply(metadata, MetadataBackend::write);
                }
            });

        container.register(NativeRpcProtocol.METADATA_DELETE_SERIES,
            new NativeRpcEndpoint<GroupedQuery<DeleteSeries.Request>, DeleteSeries>() {
                @Override
                public AsyncFuture<DeleteSeries> handle(final GroupedQuery<DeleteSeries.Request> g)
                    throws Exception {
                    return g.apply(metadata, MetadataBackend::deleteSeries);
                }
            });

        container.register(NativeRpcProtocol.SUGGEST_TAG_KEY_COUNT,
            new NativeRpcEndpoint<GroupedQuery<TagKeyCount.Request>, TagKeyCount>() {
                @Override
                public AsyncFuture<TagKeyCount> handle(final GroupedQuery<TagKeyCount.Request> g)
                    throws Exception {
                    return g.apply(suggest, SuggestBackend::tagKeyCount);
                }
            });

        container.register(NativeRpcProtocol.SUGGEST_TAG,
            new NativeRpcEndpoint<GroupedQuery<TagSuggest.Request>, TagSuggest>() {
                @Override
                public AsyncFuture<TagSuggest> handle(final GroupedQuery<TagSuggest.Request> g)
                    throws Exception {
                    return g.apply(suggest, SuggestBackend::tagSuggest);
                }
            });

        container.register(NativeRpcProtocol.SUGGEST_KEY,
            new NativeRpcEndpoint<GroupedQuery<KeySuggest.Request>, KeySuggest>() {
                @Override
                public AsyncFuture<KeySuggest> handle(final GroupedQuery<KeySuggest.Request> g)
                    throws Exception {
                    return g.apply(suggest, SuggestBackend::keySuggest);
                }
            });

        container.register(NativeRpcProtocol.SUGGEST_TAG_VALUES,
            new NativeRpcEndpoint<GroupedQuery<TagValuesSuggest.Request>, TagValuesSuggest>() {
                @Override
                public AsyncFuture<TagValuesSuggest> handle(
                    final GroupedQuery<TagValuesSuggest.Request> g
                ) throws Exception {
                    return g.apply(suggest, SuggestBackend::tagValuesSuggest);
                }
            });

        container.register(NativeRpcProtocol.SUGGEST_TAG_VALUE,
            new NativeRpcEndpoint<GroupedQuery<TagValueSuggest.Request>, TagValueSuggest>() {
                @Override
                public AsyncFuture<TagValueSuggest> handle(
                    final GroupedQuery<TagValueSuggest.Request> g
                ) throws Exception {
                    return g.apply(suggest, SuggestBackend::tagValueSuggest);
                }
            });
    }

    private AsyncFuture<Void> start() {
        final ServerBootstrap s = new ServerBootstrap();
        s.channel(NioServerSocketChannel.class);
        s.group(bossGroup, workerGroup);
        s.childHandler(
            new NativeRpcServerSession(timer, mapper, container, maxFrameSize, encoding));

        final ChannelFuture bind = s.bind(address);

        bind.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture f) throws Exception {
                if (!f.isSuccess()) {
                    bindFuture.fail(f.cause());
                    return;
                }

                serverChannel.set(f.channel());
                final InetSocketAddress address = (InetSocketAddress) f.channel().localAddress();
                bindFuture.resolve(address);
            }
        });

        return bindFuture.directTransform(a -> null);
    }

    private Callable<Void> teardownServer() {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                final Channel ch = serverChannel.getAndSet(null);

                if (ch != null) {
                    ch.close();
                }

                return null;
            }
        };
    }

    private List<AsyncFuture<Void>> teardownThreadpools() {
        final ResolvableFuture<Void> worker = async.future();

        workerGroup.shutdownGracefully().addListener(new FutureListener<Object>() {
            @Override
            public void operationComplete(Future<Object> f) throws Exception {
                if (!f.isSuccess()) {
                    worker.fail(f.cause());
                    return;
                }

                worker.resolve(null);
            }
        });

        final ResolvableFuture<Void> boss = async.future();

        bossGroup.shutdownGracefully().addListener(new FutureListener<Object>() {
            @Override
            public void operationComplete(Future<Object> f) throws Exception {
                if (!f.isSuccess()) {
                    boss.fail(f.cause());
                    return;
                }

                boss.resolve(null);
            }
        });

        return ImmutableList.<AsyncFuture<Void>>of(worker, boss);
    }

    private Callable<Void> teardownTimer() {
        return new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                timer.stop();
                return null;
            }
        };
    }

    private AsyncFuture<Void> stop() {
        final List<AsyncFuture<Void>> callbacks = new ArrayList<>();
        callbacks.add(async.call(teardownServer()));
        callbacks.addAll(teardownThreadpools());
        callbacks.add(async.call(teardownTimer()));
        return async.collectAndDiscard(callbacks);
    }
}
