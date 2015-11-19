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

import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.name.Named;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.common.LifeCycle;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.ResultGroups;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.rpc.nativerpc.NativeRpcProtocol.GroupedQuery;
import com.spotify.heroic.rpc.nativerpc.NativeRpcProtocol.RpcKeySuggest;
import com.spotify.heroic.rpc.nativerpc.NativeRpcProtocol.RpcQuery;
import com.spotify.heroic.rpc.nativerpc.NativeRpcProtocol.RpcSuggestTagValue;
import com.spotify.heroic.rpc.nativerpc.NativeRpcProtocol.RpcSuggestTagValues;
import com.spotify.heroic.rpc.nativerpc.NativeRpcProtocol.RpcTagSuggest;
import com.spotify.heroic.rpc.nativerpc.NativeRpcProtocol.RpcWriteSeries;
import com.spotify.heroic.rpc.nativerpc.message.NativeRpcEmptyBody;
import com.spotify.heroic.suggest.KeySuggest;
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
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class NativeRpcProtocolServer implements LifeCycle {
    @Inject
    private AsyncFramework async;

    @Inject
    private MetricManager metrics;

    @Inject
    private MetadataManager metadata;

    @Inject
    private SuggestManager suggest;

    @Inject
    private NodeMetadata localMetadata;

    @Inject
    @Named("application/json+internal")
    private ObjectMapper mapper;

    @Inject
    private Timer timer;

    @Inject
    @Named("boss")
    private EventLoopGroup bossGroup;

    @Inject
    @Named("worker")
    private EventLoopGroup workerGroup;

    @Inject
    private NativeEncoding encoding;

    private final SocketAddress address;
    private final int maxFrameSize;

    private final AtomicReference<Channel> serverChannel = new AtomicReference<>();
    private final NativeRpcContainer container = new NativeRpcContainer();

    {
        container.register("metadata", new NativeRpcEndpoint<NativeRpcEmptyBody, NodeMetadata>() {
            @Override
            public AsyncFuture<NodeMetadata> handle(final NativeRpcEmptyBody request)
                    throws Exception {
                return async.resolved(localMetadata);
            }
        });

        container.register(NativeRpcProtocol.METRICS_QUERY,
                new NativeRpcEndpoint<GroupedQuery<RpcQuery>, ResultGroups>() {
                    @Override
                    public AsyncFuture<ResultGroups> handle(final GroupedQuery<RpcQuery> grouped)
                            throws Exception {
                        final RpcQuery query = grouped.getQuery();

                        return metrics.useGroup(grouped.getGroup()).query(query.getSource(),
                                query.getFilter(), query.getRange(), query.getAggregation(),
                                query.getOptions());
                    }
                });

        container.register(NativeRpcProtocol.METRICS_WRITE,
                new NativeRpcEndpoint<GroupedQuery<WriteMetric>, WriteResult>() {
                    @Override
                    public AsyncFuture<WriteResult> handle(final GroupedQuery<WriteMetric> grouped)
                            throws Exception {
                        final WriteMetric query = grouped.getQuery();
                        return metrics.useGroup(grouped.getGroup()).write(query);
                    }
                });

        container.register(NativeRpcProtocol.METADATA_FIND_TAGS,
                new NativeRpcEndpoint<GroupedQuery<RangeFilter>, FindTags>() {
                    @Override
                    public AsyncFuture<FindTags> handle(final GroupedQuery<RangeFilter> grouped)
                            throws Exception {
                        return metadata.useGroup(grouped.getGroup()).findTags(grouped.getQuery());
                    }
                });

        container.register(NativeRpcProtocol.METADATA_FIND_KEYS,
                new NativeRpcEndpoint<GroupedQuery<RangeFilter>, FindKeys>() {
                    @Override
                    public AsyncFuture<FindKeys> handle(final GroupedQuery<RangeFilter> grouped)
                            throws Exception {
                        return metadata.useGroup(grouped.getGroup()).findKeys(grouped.getQuery());
                    }
                });

        container.register(NativeRpcProtocol.METADATA_FIND_SERIES,
                new NativeRpcEndpoint<GroupedQuery<RangeFilter>, FindSeries>() {
                    @Override
                    public AsyncFuture<FindSeries> handle(final GroupedQuery<RangeFilter> grouped)
                            throws Exception {
                        return metadata.useGroup(grouped.getGroup()).findSeries(grouped.getQuery());
                    }
                });

        container.register(NativeRpcProtocol.METADATA_COUNT_SERIES,
                new NativeRpcEndpoint<GroupedQuery<RangeFilter>, CountSeries>() {
                    @Override
                    public AsyncFuture<CountSeries> handle(final GroupedQuery<RangeFilter> grouped)
                            throws Exception {
                        return metadata.useGroup(grouped.getGroup())
                                .countSeries(grouped.getQuery());
                    }
                });

        container.register(NativeRpcProtocol.METADATA_WRITE,
                new NativeRpcEndpoint<GroupedQuery<RpcWriteSeries>, WriteResult>() {
                    @Override
                    public AsyncFuture<WriteResult> handle(
                            final GroupedQuery<RpcWriteSeries> grouped) throws Exception {
                        final RpcWriteSeries query = grouped.getQuery();
                        return metadata.useGroup(grouped.getGroup()).write(query.getSeries(),
                                query.getRange());
                    }
                });

        container.register(NativeRpcProtocol.METADATA_DELETE_SERIES,
                new NativeRpcEndpoint<GroupedQuery<RangeFilter>, DeleteSeries>() {
                    @Override
                    public AsyncFuture<DeleteSeries> handle(final GroupedQuery<RangeFilter> grouped)
                            throws Exception {
                        return metadata.useGroup(grouped.getGroup())
                                .deleteSeries(grouped.getQuery());
                    }
                });

        container.register(NativeRpcProtocol.SUGGEST_TAG_KEY_COUNT,
                new NativeRpcEndpoint<GroupedQuery<RangeFilter>, TagKeyCount>() {
                    @Override
                    public AsyncFuture<TagKeyCount> handle(final GroupedQuery<RangeFilter> grouped)
                            throws Exception {
                        return suggest.useGroup(grouped.getGroup()).tagKeyCount(grouped.getQuery());
                    }
                });

        container.register(NativeRpcProtocol.SUGGEST_TAG,
                new NativeRpcEndpoint<GroupedQuery<RpcTagSuggest>, TagSuggest>() {
                    @Override
                    public AsyncFuture<TagSuggest> handle(final GroupedQuery<RpcTagSuggest> grouped)
                            throws Exception {
                        final RpcTagSuggest query = grouped.getQuery();
                        return suggest.useGroup(grouped.getGroup()).tagSuggest(query.getFilter(),
                                query.getMatch(), query.getKey(), query.getValue());
                    }
                });

        container.register(NativeRpcProtocol.SUGGEST_KEY,
                new NativeRpcEndpoint<GroupedQuery<RpcKeySuggest>, KeySuggest>() {
                    @Override
                    public AsyncFuture<KeySuggest> handle(final GroupedQuery<RpcKeySuggest> grouped)
                            throws Exception {
                        final RpcKeySuggest query = grouped.getQuery();
                        return suggest.useGroup(grouped.getGroup()).keySuggest(query.getFilter(),
                                query.getMatch(), query.getKey());
                    }
                });

        container.register(NativeRpcProtocol.SUGGEST_TAG_VALUES,
                new NativeRpcEndpoint<GroupedQuery<RpcSuggestTagValues>, TagValuesSuggest>() {
                    @Override
                    public AsyncFuture<TagValuesSuggest> handle(
                            final GroupedQuery<RpcSuggestTagValues> grouped) throws Exception {
                        final RpcSuggestTagValues query = grouped.getQuery();
                        return suggest.useGroup(grouped.getGroup()).tagValuesSuggest(
                                query.getFilter(), query.getExclude(), query.getGroupLimit());
                    }
                });

        container.register(NativeRpcProtocol.SUGGEST_TAG_VALUE,
                new NativeRpcEndpoint<GroupedQuery<RpcSuggestTagValue>, TagValueSuggest>() {
                    @Override
                    public AsyncFuture<TagValueSuggest> handle(
                            final GroupedQuery<RpcSuggestTagValue> grouped) throws Exception {
                        final RpcSuggestTagValue query = grouped.getQuery();
                        return suggest.useGroup(grouped.getGroup())
                                .tagValueSuggest(query.getFilter(), query.getKey());
                    }
                });
    }

    @Override
    public AsyncFuture<Void> start() {
        final ServerBootstrap s = new ServerBootstrap();
        s.channel(NioServerSocketChannel.class);
        s.group(bossGroup, workerGroup);
        s.childHandler(
                new NativeRpcServerSession(timer, mapper, container, maxFrameSize, encoding));

        final ResolvableFuture<Void> bindFuture = async.future();

        final ChannelFuture bind = s.bind(address);

        bind.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(final ChannelFuture f) throws Exception {
                if (!f.isSuccess()) {
                    bindFuture.fail(f.cause());
                    return;
                }

                serverChannel.set(f.channel());
                bindFuture.resolve(null);
            }
        });

        return bindFuture;
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

        return ImmutableList.<AsyncFuture<Void>> of(worker, boss);
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

    @Override
    public AsyncFuture<Void> stop() {
        final List<AsyncFuture<Void>> callbacks = new ArrayList<>();
        callbacks.add(async.call(teardownServer()));
        callbacks.addAll(teardownThreadpools());
        callbacks.add(async.call(teardownTimer()));
        return async.collectAndDiscard(callbacks);
    }

    @Override
    public boolean isReady() {
        return true;
    }
}
