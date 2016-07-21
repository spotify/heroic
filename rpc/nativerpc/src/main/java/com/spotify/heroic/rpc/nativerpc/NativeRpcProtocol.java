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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.RpcProtocol;
import com.spotify.heroic.cluster.TracingClusterNodeGroup;
import com.spotify.heroic.common.Grouped;
import com.spotify.heroic.common.UsableGroupManager;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.util.Timer;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import javax.inject.Inject;
import javax.inject.Named;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkNotNull;

@ToString(of = {})
public class NativeRpcProtocol implements RpcProtocol {
    public static final String METADATA = "metadata";
    public static final String METRICS_QUERY = "metrics:query";
    public static final String METRICS_WRITE = "metrics:write";
    public static final String METADATA_FIND_TAGS = "metadata:findTags";
    public static final String METADATA_FIND_KEYS = "metadata:findKeys";
    public static final String METADATA_FIND_SERIES = "metadata:findSeries";
    public static final String METADATA_COUNT_SERIES = "metadata:countSeries";
    public static final String METADATA_DELETE_SERIES = "metadata:deleteSeries";
    public static final String METADATA_WRITE = "metadata:writeSeries";
    public static final String SUGGEST_TAG_KEY_COUNT = "suggest:tagKeyCount";
    public static final String SUGGEST_KEY = "suggest:key";
    public static final String SUGGEST_TAG = "suggest:tag";
    public static final String SUGGEST_TAG_VALUES = "suggest:tagValues";
    public static final String SUGGEST_TAG_VALUE = "suggest:tagValue";

    private final AsyncFramework async;
    private final EventLoopGroup workerGroup;
    private final ObjectMapper mapper;
    private final Timer timer;
    private final NativeEncoding encoding;
    private final ResolvableFuture<InetSocketAddress> bindFuture;

    private final int defaultPort;
    private final int maxFrameSize;
    private final long sendTimeout;
    private final long heartbeatReadInterval;

    @Inject
    public NativeRpcProtocol(
        AsyncFramework async, @Named("worker") EventLoopGroup workerGroup,
        @Named("application/json+internal") ObjectMapper mapper, Timer timer,
        NativeEncoding encoding,
        @Named("bindFuture") ResolvableFuture<InetSocketAddress> bindFuture,
        @Named("defaultPort") int defaultPort, @Named("maxFrameSize") int maxFrameSize,
        @Named("sendTimeout") long sendTimeout,
        @Named("heartbeatReadInterval") long heartbeatReadInterval
    ) {
        this.async = async;
        this.workerGroup = workerGroup;
        this.mapper = mapper;
        this.timer = timer;
        this.encoding = encoding;
        this.bindFuture = bindFuture;
        this.defaultPort = defaultPort;
        this.maxFrameSize = maxFrameSize;
        this.sendTimeout = sendTimeout;
        this.heartbeatReadInterval = heartbeatReadInterval;
    }

    @Override
    public AsyncFuture<ClusterNode> connect(final URI uri) {
        final InetSocketAddress address =
            new InetSocketAddress(uri.getHost(), uri.getPort() == -1 ? defaultPort : uri.getPort());
        final NativeRpcClient client =
            new NativeRpcClient(async, workerGroup, maxFrameSize, address, mapper, timer,
                sendTimeout, heartbeatReadInterval, encoding);

        return client
            .request(METADATA, NodeMetadata.class)
            .directTransform(m -> new NativeRpcClusterNode(uri, client, m));
    }

    @Override
    public AsyncFuture<String> getListenURI() {
        return bindFuture.directTransform(s -> {
            if (s.getAddress() instanceof Inet6Address) {
                return String.format("nativerpc://[%s]:%d", s.getAddress().getHostAddress(),
                    s.getPort());
            }

            return String.format("nativerpc://%s:%d", s.getHostString(), s.getPort());
        });
    }

    @RequiredArgsConstructor
    public class NativeRpcClusterNode implements ClusterNode {
        private final URI uri;
        private final NativeRpcClient client;
        private final NodeMetadata metadata;

        @Override
        public NodeMetadata metadata() {
            return metadata;
        }

        @Override
        public AsyncFuture<NodeMetadata> fetchMetadata() {
            return client.request(METADATA, NodeMetadata.class);
        }

        @Override
        public AsyncFuture<Void> close() {
            return async.resolved(null);
        }

        @Override
        public ClusterNode.Group useOptionalGroup(Optional<String> group) {
            return new TracingClusterNodeGroup(uri.toString(), new Group(group));
        }

        @Override
        public String toString() {
            return client.toString();
        }

        @RequiredArgsConstructor
        private class Group implements ClusterNode.Group {
            private final Optional<String> group;

            @Override
            public ClusterNode node() {
                return NativeRpcClusterNode.this;
            }

            @Override
            public AsyncFuture<FullQuery> query(final FullQuery.Request request) {
                return request(METRICS_QUERY, request, FullQuery.class);
            }

            @Override
            public AsyncFuture<WriteMetric> writeMetric(WriteMetric.Request request) {
                return request(METRICS_WRITE, request, WriteMetric.class);
            }

            @Override
            public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
                return request(METADATA_FIND_TAGS, request, FindTags.class);
            }

            @Override
            public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
                return request(METADATA_FIND_KEYS, request, FindKeys.class);
            }

            @Override
            public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
                return request(METADATA_FIND_SERIES, request, FindSeries.class);
            }

            @Override
            public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
                return request(METADATA_COUNT_SERIES, request, CountSeries.class);
            }

            @Override
            public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
                return request(METADATA_DELETE_SERIES, request, DeleteSeries.class);
            }

            @Override
            public AsyncFuture<WriteMetadata> writeSeries(final WriteMetadata.Request request) {
                return request(METADATA_WRITE, request, WriteMetadata.class);
            }

            @Override
            public AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request) {
                return request(SUGGEST_TAG_KEY_COUNT, request, TagKeyCount.class);
            }

            @Override
            public AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request) {
                return request(SUGGEST_TAG, request, TagSuggest.class);
            }

            @Override
            public AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request) {
                return request(SUGGEST_KEY, request, KeySuggest.class);
            }

            @Override
            public AsyncFuture<TagValuesSuggest> tagValuesSuggest(
                final TagValuesSuggest.Request request
            ) {
                return request(SUGGEST_TAG_VALUES, request, TagValuesSuggest.class);
            }

            @Override
            public AsyncFuture<TagValueSuggest> tagValueSuggest(
                final TagValueSuggest.Request request
            ) {
                return request(SUGGEST_TAG_VALUE, request, TagValueSuggest.class);
            }

            private <T, R> AsyncFuture<R> request(String endpoint, T body, Class<R> expected) {
                final GroupedQuery<T> grouped = new GroupedQuery<>(group, body);
                return client.request(endpoint, grouped, expected);
            }
        }
    }

    @Data
    public static class GroupedQuery<T> {
        private final Optional<String> group;
        private final T query;

        @JsonCreator
        public GroupedQuery(
            @JsonProperty("group") Optional<String> group, @JsonProperty("query") T query
        ) {
            this.group = group;
            this.query = checkNotNull(query, "query");
        }

        public <G extends Grouped, R> R apply(
            UsableGroupManager<G> manager, BiFunction<G, T, R> function
        ) {
            return function.apply(group.map(manager::useGroup).orElseGet(manager::useDefaultGroup),
                query);
        }
    }
}
