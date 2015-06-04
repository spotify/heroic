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

import static com.google.common.base.Preconditions.checkNotNull;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.Timer;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.FutureListener;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.name.Named;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.RpcProtocol;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.injection.LifeCycle;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.model.CountSeries;
import com.spotify.heroic.metadata.model.DeleteSeries;
import com.spotify.heroic.metadata.model.FindKeys;
import com.spotify.heroic.metadata.model.FindSeries;
import com.spotify.heroic.metadata.model.FindTags;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.model.ResultGroups;
import com.spotify.heroic.metric.model.WriteMetric;
import com.spotify.heroic.metric.model.WriteResult;
import com.spotify.heroic.model.DateRange;
import com.spotify.heroic.model.RangeFilter;
import com.spotify.heroic.model.Series;
import com.spotify.heroic.model.TimeData;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.model.KeySuggest;
import com.spotify.heroic.suggest.model.MatchOptions;
import com.spotify.heroic.suggest.model.TagKeyCount;
import com.spotify.heroic.suggest.model.TagSuggest;
import com.spotify.heroic.suggest.model.TagValueSuggest;
import com.spotify.heroic.suggest.model.TagValuesSuggest;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;

@ToString(of = {})
@RequiredArgsConstructor
public class NativeRpcProtocol implements RpcProtocol, LifeCycle {
    private static final String METADATA = "metadata";
    private static final String METRICS_QUERY = "metrics:query";
    private static final String METRICS_WRITE = "metrics:write";
    private static final String METADATA_FIND_TAGS = "metadata:findTags";
    private static final String METADATA_FIND_KEYS = "metadata:findKeys";
    private static final String METADATA_FIND_SERIES = "metadata:findSeries";
    private static final String METADATA_COUNT_SERIES = "metadata:countSeries";
    private static final String METADATA_DELETE_SERIES = "metadata:deleteSeries";
    private static final String METADATA_WRITE = "metadata:writeSeries";
    private static final String SUGGEST_TAG_KEY_COUNT = "suggest:tagKeyCount";
    private static final String SUGGEST_KEY = "suggest:key";
    private static final String SUGGEST_TAG = "suggest:tag";
    private static final String SUGGEST_TAG_VALUES = "suggest:tagValues";
    private static final String SUGGEST_TAG_VALUE = "suggest:tagValue";

    @Inject
    private AsyncFramework async;

    @Inject
    private MetricManager metrics;

    @Inject
    private MetadataManager metadata;

    @Inject
    private SuggestManager suggest;

    @Inject
    @Named("boss")
    private EventLoopGroup bossGroup;

    @Inject
    @Named("worker")
    private EventLoopGroup workerGroup;

    @Inject
    private NodeMetadata localMetadata;

    @Inject
    @Named("application/json+internal")
    private ObjectMapper mapper;

    @Inject
    private Timer timer;

    private final SocketAddress address;
    private final int defaultPort;
    private final int maxFrameSize;
    private final long sendTimeout;
    private final long heartbeatInterval;
    private final long heartbeatReadInterval;

    private final AtomicReference<Channel> serverChannel = new AtomicReference<>();
    private final NativeRpcContainer container = new NativeRpcContainer();

    {
        container.register("metadata", new NativeRpcContainer.Endpoint<RpcEmptyBody, NodeMetadata>(
                new TypeReference<RpcEmptyBody>() {
                }) {
            @Override
            public AsyncFuture<NodeMetadata> handle(final RpcEmptyBody request) throws Exception {
                return async.resolved(localMetadata);
            }
        });

        container.register(METRICS_QUERY, new NativeRpcContainer.Endpoint<RpcGroupedQuery<RpcQuery>, ResultGroups>(
                new TypeReference<RpcGroupedQuery<RpcQuery>>() {
                }) {
            @Override
            public AsyncFuture<ResultGroups> handle(final RpcGroupedQuery<RpcQuery> grouped) throws Exception {
                final RpcQuery query = grouped.getQuery();

                return metrics.useGroup(grouped.getGroup()).query(query.getSource(), query.getFilter(),
                        query.getGroupBy(), query.getRange(), query.getAggregation(), query.isNoCache());
            }
        });

        container.register(METRICS_WRITE, new NativeRpcContainer.Endpoint<RpcGroupedQuery<WriteMetric>, WriteResult>(
                new TypeReference<RpcGroupedQuery<WriteMetric>>() {
                }) {
            @Override
            public AsyncFuture<WriteResult> handle(final RpcGroupedQuery<WriteMetric> grouped) throws Exception {
                final WriteMetric query = grouped.getQuery();
                return metrics.useGroup(grouped.getGroup()).write(query);
            }
        });

        container.register(METADATA_FIND_TAGS, new NativeRpcContainer.Endpoint<RpcGroupedQuery<RangeFilter>, FindTags>(
                new TypeReference<RpcGroupedQuery<RangeFilter>>() {
                }) {
            @Override
            public AsyncFuture<FindTags> handle(final RpcGroupedQuery<RangeFilter> grouped) throws Exception {
                return metadata.useGroup(grouped.getGroup()).findTags(grouped.getQuery());
            }
        });

        container.register(METADATA_FIND_KEYS, new NativeRpcContainer.Endpoint<RpcGroupedQuery<RangeFilter>, FindKeys>(
                new TypeReference<RpcGroupedQuery<RangeFilter>>() {
                }) {
            @Override
            public AsyncFuture<FindKeys> handle(final RpcGroupedQuery<RangeFilter> grouped) throws Exception {
                return metadata.useGroup(grouped.getGroup()).findKeys(grouped.getQuery());
            }
        });

        container.register(METADATA_FIND_SERIES,
                new NativeRpcContainer.Endpoint<RpcGroupedQuery<RangeFilter>, FindSeries>(
                        new TypeReference<RpcGroupedQuery<RangeFilter>>() {
                        }) {
                    @Override
                    public AsyncFuture<FindSeries> handle(final RpcGroupedQuery<RangeFilter> grouped) throws Exception {
                        return metadata.useGroup(grouped.getGroup()).findSeries(grouped.getQuery());
                    }
                });

        container.register(METADATA_COUNT_SERIES,
                new NativeRpcContainer.Endpoint<RpcGroupedQuery<RangeFilter>, CountSeries>(
                        new TypeReference<RpcGroupedQuery<RangeFilter>>() {
                        }) {
                    @Override
                    public AsyncFuture<CountSeries> handle(final RpcGroupedQuery<RangeFilter> grouped) throws Exception {
                        return metadata.useGroup(grouped.getGroup()).countSeries(grouped.getQuery());
                    }
                });

        container.register(METADATA_WRITE,
                new NativeRpcContainer.Endpoint<RpcGroupedQuery<RpcWriteSeries>, WriteResult>(
                        new TypeReference<RpcGroupedQuery<RpcWriteSeries>>() {
                        }) {
                    @Override
                    public AsyncFuture<WriteResult> handle(final RpcGroupedQuery<RpcWriteSeries> grouped)
                            throws Exception {
                        final RpcWriteSeries query = grouped.getQuery();
                        return metadata.useGroup(grouped.getGroup()).write(query.getSeries(), query.getRange());
                    }
                });

        container.register(METADATA_DELETE_SERIES,
                new NativeRpcContainer.Endpoint<RpcGroupedQuery<RangeFilter>, DeleteSeries>(
                        new TypeReference<RpcGroupedQuery<RangeFilter>>() {
                        }) {
                    @Override
                    public AsyncFuture<DeleteSeries> handle(final RpcGroupedQuery<RangeFilter> grouped)
                            throws Exception {
                        return metadata.useGroup(grouped.getGroup()).deleteSeries(grouped.getQuery());
                    }
                });

        container.register(SUGGEST_TAG_KEY_COUNT,
                new NativeRpcContainer.Endpoint<RpcGroupedQuery<RangeFilter>, TagKeyCount>(
                        new TypeReference<RpcGroupedQuery<RangeFilter>>() {
                        }) {
                    @Override
                    public AsyncFuture<TagKeyCount> handle(final RpcGroupedQuery<RangeFilter> grouped) throws Exception {
                        return suggest.useGroup(grouped.getGroup()).tagKeyCount(grouped.getQuery());
                    }
                });

        container.register(SUGGEST_TAG, new NativeRpcContainer.Endpoint<RpcGroupedQuery<RpcTagSuggest>, TagSuggest>(
                new TypeReference<RpcGroupedQuery<RpcTagSuggest>>() {
                }) {
            @Override
            public AsyncFuture<TagSuggest> handle(final RpcGroupedQuery<RpcTagSuggest> grouped) throws Exception {
                final RpcTagSuggest query = grouped.getQuery();
                return suggest.useGroup(grouped.getGroup()).tagSuggest(query.getFilter(), query.getMatch(),
                        query.getKey(), query.getValue());
            }
        });

        container.register(SUGGEST_KEY, new NativeRpcContainer.Endpoint<RpcGroupedQuery<RpcKeySuggest>, KeySuggest>(
                new TypeReference<RpcGroupedQuery<RpcKeySuggest>>() {
                }) {
            @Override
            public AsyncFuture<KeySuggest> handle(final RpcGroupedQuery<RpcKeySuggest> grouped) throws Exception {
                final RpcKeySuggest query = grouped.getQuery();
                return suggest.useGroup(grouped.getGroup()).keySuggest(query.getFilter(), query.getMatch(),
                        query.getKey());
            }
        });

        container.register(SUGGEST_TAG_VALUES,
                new NativeRpcContainer.Endpoint<RpcGroupedQuery<RpcSuggestTagValues>, TagValuesSuggest>(
                        new TypeReference<RpcGroupedQuery<RpcSuggestTagValues>>() {
                        }) {
                    @Override
                    public AsyncFuture<TagValuesSuggest> handle(final RpcGroupedQuery<RpcSuggestTagValues> grouped)
                            throws Exception {
                        final RpcSuggestTagValues query = grouped.getQuery();
                        return suggest.useGroup(grouped.getGroup()).tagValuesSuggest(query.getFilter(),
                                query.getExclude(), query.getGroupLimit());
                    }
                });

        container.register(SUGGEST_TAG_VALUE,
                new NativeRpcContainer.Endpoint<RpcGroupedQuery<RpcSuggestTagValue>, TagValueSuggest>(
                        new TypeReference<RpcGroupedQuery<RpcSuggestTagValue>>() {
                        }) {
                    @Override
                    public AsyncFuture<TagValueSuggest> handle(final RpcGroupedQuery<RpcSuggestTagValue> grouped)
                            throws Exception {
                        final RpcSuggestTagValue query = grouped.getQuery();
                        return suggest.useGroup(grouped.getGroup()).tagValueSuggest(query.getFilter(), query.getKey());
                    }
                });
    }

    @Override
    public AsyncFuture<Void> start() throws Exception {
        final ServerBootstrap s = new ServerBootstrap();
        s.channel(NioServerSocketChannel.class);
        s.group(bossGroup, workerGroup);
        s.childHandler(new NativeRpcServerSessionInitializer(timer, mapper, container, heartbeatInterval, maxFrameSize));

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

                if (ch != null)
                    ch.close();

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
    public AsyncFuture<Void> stop() throws Exception {
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

    @Override
    public AsyncFuture<ClusterNode> connect(final URI uri) {
        final InetSocketAddress address = new InetSocketAddress(uri.getHost(), uri.getPort() == -1 ? defaultPort
                : uri.getPort());
        final NativeRpcClient client = new NativeRpcClient(async, workerGroup, maxFrameSize, address, mapper, timer,
                sendTimeout, heartbeatReadInterval);

        return client.request(METADATA, NodeMetadata.class).transform(new Transform<NodeMetadata, ClusterNode>() {
            @Override
            public ClusterNode transform(final NodeMetadata metadata) throws Exception {
                return new NativeRpcClusterNode(client, metadata);
            }
        });
    }

    @ToString(of = { "client", "metadata" })
    @RequiredArgsConstructor
    public class NativeRpcClusterNode implements ClusterNode {
        private final NativeRpcClient client;
        private final NodeMetadata metadata;

        @Override
        public NodeMetadata metadata() {
            return metadata;
        }

        @Override
        public AsyncFuture<Void> close() {
            return async.resolved(null);
        }

        @Override
        public Group useGroup(String group) {
            return new Group(group);
        }

        @RequiredArgsConstructor
        private class Group implements ClusterNode.Group {
            private final String group;

            @Override
            public AsyncFuture<ResultGroups> query(Class<? extends TimeData> source, Filter filter,
                    List<String> groupBy, DateRange range, Aggregation aggregation, boolean disableCache) {
                return request(METRICS_QUERY, new RpcQuery(source, filter, groupBy, range, aggregation, disableCache),
                        ResultGroups.class);
            }

            @Override
            public AsyncFuture<WriteResult> writeMetric(WriteMetric write) {
                return request(METRICS_WRITE, write, WriteResult.class);
            }

            @Override
            public AsyncFuture<FindTags> findTags(RangeFilter filter) {
                return request(METADATA_FIND_TAGS, filter, FindTags.class);
            }

            @Override
            public AsyncFuture<FindKeys> findKeys(RangeFilter filter) {
                return request(METADATA_FIND_KEYS, filter, FindKeys.class);
            }

            @Override
            public AsyncFuture<FindSeries> findSeries(RangeFilter filter) {
                return request(METADATA_FIND_SERIES, filter, FindSeries.class);
            }

            @Override
            public AsyncFuture<CountSeries> countSeries(RangeFilter filter) {
                return request(METADATA_COUNT_SERIES, filter, CountSeries.class);
            }

            @Override
            public AsyncFuture<DeleteSeries> deleteSeries(RangeFilter filter) {
                return request(METADATA_DELETE_SERIES, filter, DeleteSeries.class);
            }

            @Override
            public AsyncFuture<WriteResult> writeSeries(DateRange range, Series series) {
                return request(METADATA_WRITE, new RpcWriteSeries(range, series), WriteResult.class);
            }

            @Override
            public AsyncFuture<TagKeyCount> tagKeyCount(RangeFilter filter) {
                return request(SUGGEST_TAG_KEY_COUNT, filter, TagKeyCount.class);
            }

            @Override
            public AsyncFuture<TagSuggest> tagSuggest(RangeFilter filter, MatchOptions match, String key, String value) {
                return request(SUGGEST_TAG, new RpcTagSuggest(filter, match, key, value), TagSuggest.class);
            }

            @Override
            public AsyncFuture<KeySuggest> keySuggest(RangeFilter filter, MatchOptions match, String key) {
                return request(SUGGEST_KEY, new RpcKeySuggest(filter, match, key), KeySuggest.class);
            }

            @Override
            public AsyncFuture<TagValuesSuggest> tagValuesSuggest(RangeFilter filter, List<String> exclude,
                    int groupLimit) {
                return request(SUGGEST_TAG_VALUES, new RpcSuggestTagValues(filter, exclude, groupLimit),
                        TagValuesSuggest.class);
            }

            @Override
            public AsyncFuture<TagValueSuggest> tagValueSuggest(RangeFilter filter, String key) {
                return request(SUGGEST_TAG_VALUE, new RpcSuggestTagValue(filter, key), TagValueSuggest.class);
            }

            private <T, R> AsyncFuture<R> request(String endpoint, T body, Class<R> expected) {
                final RpcGroupedQuery<T> grouped = new RpcGroupedQuery<>(group, body);
                return client.request(endpoint, grouped, expected);
            }
        }
    }

    @Data
    public static class RpcGroupedQuery<T> {
        private final String group;
        private final T query;

        @JsonCreator
        public RpcGroupedQuery(@JsonProperty("group") String group, @JsonProperty("query") T query) {
            this.group = group;
            this.query = query;
        }
    }

    @Data
    public static class RpcQuery {
        private final Class<? extends TimeData> source;
        private final Filter filter;
        private final List<String> groupBy;
        private final DateRange range;
        private final Aggregation aggregation;
        private final boolean noCache;

        @JsonCreator
        public RpcQuery(@JsonProperty("source") final Class<? extends TimeData> source,
                @JsonProperty("filter") final Filter filter, @JsonProperty("groupBy") final List<String> groupBy,
                @JsonProperty("range") final DateRange range,
                @JsonProperty("aggregation") final Aggregation aggregation,
                @JsonProperty("noCache") final Boolean noCache) {
            this.source = checkNotNull(source, "source must not be null");
            this.filter = filter;
            this.groupBy = groupBy;
            this.range = checkNotNull(range, "range must not be null");
            this.aggregation = aggregation;
            this.noCache = checkNotNull(noCache, "noCache must not be null");
        }
    }

    @Data
    public static class RpcTagSuggest {
        private final RangeFilter filter;
        private final MatchOptions match;
        private final String key;
        private final String value;

        public RpcTagSuggest(@JsonProperty("range") final RangeFilter filter,
                @JsonProperty("match") final MatchOptions match, @JsonProperty("key") final String key,
                @JsonProperty("value") final String value) {
            this.filter = filter;
            this.match = checkNotNull(match, "match options must not be null");
            this.key = key;
            this.value = value;
        }
    }

    @Data
    public static class RpcSuggestTagValues {
        private final RangeFilter filter;
        private final List<String> exclude;
        private final int groupLimit;

        public RpcSuggestTagValues(@JsonProperty("range") final RangeFilter filter,
                @JsonProperty("exclude") final List<String> exclude,
                @JsonProperty("groupLimit") final Integer groupLimit) {
            this.filter = filter;
            this.exclude = checkNotNull(exclude, "exclude must not be null");
            this.groupLimit = checkNotNull(groupLimit, "groupLimit must not be null");
        }
    }

    @Data
    public static class RpcSuggestTagValue {
        private final RangeFilter filter;
        private final String key;

        public RpcSuggestTagValue(@JsonProperty("range") final RangeFilter filter, @JsonProperty("key") final String key) {
            this.filter = filter;
            this.key = checkNotNull(key, "key must not be null");
        }
    }

    @Data
    public static class RpcKeySuggest {
        private final RangeFilter filter;
        private final MatchOptions match;
        private final String key;

        public RpcKeySuggest(@JsonProperty("range") final RangeFilter filter,
                @JsonProperty("match") final MatchOptions match, @JsonProperty("key") final String key) {
            this.filter = filter;
            this.match = checkNotNull(match, "match options must not be null");
            this.key = key;
        }
    }

    @Data
    public static class RpcWriteSeries {
        private final DateRange range;
        private final Series series;

        public RpcWriteSeries(@JsonProperty("range") final DateRange range, @JsonProperty("series") final Series series) {
            this.range = checkNotNull(range);
            this.series = checkNotNull(series);
        }
    }
}
