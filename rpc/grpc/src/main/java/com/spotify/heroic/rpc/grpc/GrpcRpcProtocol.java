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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
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
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.async.ResolvableFuture;
import io.grpc.CallOptions;
import io.grpc.ManagedChannel;
import io.grpc.MethodDescriptor;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.nio.NioEventLoopGroup;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Named;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.grpc.MethodDescriptor.generateFullMethodName;

@Slf4j
@ToString(of = {})
@GrpcRpcScope
public class GrpcRpcProtocol implements RpcProtocol {
    private final AsyncFramework async;
    private final ObjectMapper mapper;
    private final ResolvableFuture<InetSocketAddress> bindFuture;

    private final int defaultPort;
    private final int maxFrameSize;
    private final NioEventLoopGroup workerGroup;

    private final Object lock = new Object();

    @Inject
    public GrpcRpcProtocol(
        AsyncFramework async, @Named("application/json+internal") ObjectMapper mapper,
        @Named("bindFuture") ResolvableFuture<InetSocketAddress> bindFuture,
        @Named("defaultPort") int defaultPort, @Named("maxFrameSize") int maxFrameSize,
        @Named("worker") NioEventLoopGroup workerGroup
    ) {
        this.async = async;
        this.mapper = mapper;
        this.bindFuture = bindFuture;
        this.defaultPort = defaultPort;
        this.maxFrameSize = maxFrameSize;
        this.workerGroup = workerGroup;
    }

    @Override
    public AsyncFuture<ClusterNode> connect(final URI uri) {
        final InetSocketAddress address =
            new InetSocketAddress(uri.getHost(), uri.getPort() == -1 ? defaultPort : uri.getPort());

        final Managed<ManagedChannel> channel = async.managed(new ManagedSetup<ManagedChannel>() {
            @Override
            public AsyncFuture<ManagedChannel> construct() throws Exception {
                final ManagedChannel channel = NettyChannelBuilder
                    .forAddress(address.getHostName(), address.getPort())
                    .usePlaintext(true)
                    .executor(workerGroup)
                    .eventLoopGroup(workerGroup)
                    .build();

                return async.resolved(channel);
            }

            @Override
            public AsyncFuture<Void> destruct(final ManagedChannel value) throws Exception {
                return async.call(() -> {
                    log.info("ManagedChannel::destruct will shutdown");
                    value.shutdown();
                    boolean didTerminate = value.awaitTermination(10, TimeUnit.SECONDS);
                    log.info("ManagedChannel::destruct has " + (didTerminate ? "" : "NOT") +
                             " terminated");
                    return null;
                });
            }
        });

        return channel.start().lazyTransform(n -> {
            final GrpcRpcClient client = new GrpcRpcClient(async, address, mapper, channel);

            return client
                .request(METADATA, CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS))
                .directTransform(m -> new GrpcRpcClusterNode(uri, client, m));
        });
    }

    @Override
    public AsyncFuture<String> getListenURI() {
        return bindFuture.directTransform(s -> {
            if (s.getAddress() instanceof Inet6Address) {
                return String.format("grpc://[%s]:%d", s.getAddress().getHostAddress(),
                    s.getPort());
            }

            return String.format("grpc://%s:%d", s.getHostString(), s.getPort());
        });
    }

    @RequiredArgsConstructor
    public class GrpcRpcClusterNode implements ClusterNode {
        private final URI uri;
        private final GrpcRpcClient client;
        private final NodeMetadata metadata;

        @Override
        public NodeMetadata metadata() {
            return metadata;
        }

        @Override
        public AsyncFuture<NodeMetadata> fetchMetadata() {
            // metadata requests are also used as health checks, used a much smaller deadline.
            return client.request(METADATA,
                CallOptions.DEFAULT.withDeadlineAfter(5, TimeUnit.SECONDS));
        }

        @Override
        public AsyncFuture<Void> close() {
            return client.close();
        }

        @Override
        public ClusterNode.Group useOptionalGroup(Optional<String> group) {
            return new TracingClusterNodeGroup(uri.toString(), new Group(group));
        }

        @Override
        public boolean isAlive() {
            return client.isAlive();
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
                return GrpcRpcClusterNode.this;
            }

            @Override
            public AsyncFuture<FullQuery> query(final FullQuery.Request request) {
                return request(METRICS_FULL_QUERY, request);
            }

            @Override
            public AsyncFuture<WriteMetric> writeMetric(final WriteMetric.Request request) {
                return request(METRICS_WRITE, request);
            }

            @Override
            public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
                return request(METADATA_FIND_TAGS, request);
            }

            @Override
            public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
                return request(METADATA_FIND_KEYS, request);
            }

            @Override
            public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
                return request(METADATA_FIND_SERIES, request);
            }

            @Override
            public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
                return request(METADATA_COUNT_SERIES, request);
            }

            @Override
            public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
                return request(METADATA_DELETE_SERIES, request);
            }

            @Override
            public AsyncFuture<WriteMetadata> writeSeries(final WriteMetadata.Request request) {
                return request(METADATA_WRITE, request);
            }

            @Override
            public AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request) {
                return request(SUGGEST_TAG_KEY_COUNT, request);
            }

            @Override
            public AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request) {
                return request(SUGGEST_TAG, request);
            }

            @Override
            public AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request) {
                return request(SUGGEST_KEY, request);
            }

            @Override
            public AsyncFuture<TagValuesSuggest> tagValuesSuggest(
                final TagValuesSuggest.Request request
            ) {
                return request(SUGGEST_TAG_VALUES, request);
            }

            @Override
            public AsyncFuture<TagValueSuggest> tagValueSuggest(TagValueSuggest.Request request) {
                return request(SUGGEST_TAG_VALUE, request);
            }

            private <T, R> AsyncFuture<R> request(
                GrpcDescriptor<GroupedQuery<T>, R> endpoint, T body
            ) {
                final GroupedQuery<T> grouped = new GroupedQuery<>(group, body);
                return client.request(endpoint, grouped, CallOptions.DEFAULT);
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

    private static final MethodDescriptor.Marshaller<byte[]> BYTE_MARSHALLER =
        new MethodDescriptor.Marshaller<byte[]>() {
            @Override
            public InputStream stream(final byte[] value) {
                return new ByteArrayInputStream(value);
            }

            @Override
            public byte[] parse(final InputStream stream) {
                try {
                    return ByteStreams.toByteArray(stream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };

    public static final String SERVICE = "heroic";

    private static <Q, R> GrpcDescriptor<Q, R> descriptor(
        final String endpointName, final TypeReference<Q> requestType,
        final TypeReference<R> responseType
    ) {
        final MethodDescriptor<byte[], byte[]> descriptor =
            MethodDescriptor.create(MethodDescriptor.MethodType.SERVER_STREAMING,
                generateFullMethodName(SERVICE, endpointName), BYTE_MARSHALLER, BYTE_MARSHALLER);

        return new GrpcRpcEndpointSpec<>(requestType, responseType, descriptor);
    }

    public static final GrpcDescriptor<GrpcRpcEmptyBody, NodeMetadata> METADATA =
        descriptor("metadata", new TypeReference<GrpcRpcEmptyBody>() {
        }, new TypeReference<NodeMetadata>() {
        });

    public static final GrpcDescriptor<GroupedQuery<FullQuery.Request>, FullQuery>
        METRICS_FULL_QUERY =
        descriptor("metrics:fullQuery", new TypeReference<GroupedQuery<FullQuery.Request>>() {
        }, new TypeReference<FullQuery>() {
        });

    public static final GrpcDescriptor<GroupedQuery<WriteMetric.Request>, WriteMetric>
        METRICS_WRITE =
        descriptor("metrics:write", new TypeReference<GroupedQuery<WriteMetric.Request>>() {
        }, new TypeReference<WriteMetric>() {
        });

    public static final GrpcDescriptor<GroupedQuery<FindTags.Request>, FindTags>
        METADATA_FIND_TAGS =
        descriptor("metadata:findTags", new TypeReference<GroupedQuery<FindTags.Request>>() {
        }, new TypeReference<FindTags>() {
        });

    public static final GrpcDescriptor<GroupedQuery<FindKeys.Request>, FindKeys>
        METADATA_FIND_KEYS =
        descriptor("metadata:findKeys", new TypeReference<GroupedQuery<FindKeys.Request>>() {
        }, new TypeReference<FindKeys>() {
        });

    public static final GrpcDescriptor<GroupedQuery<FindSeries.Request>, FindSeries>
        METADATA_FIND_SERIES =
        descriptor("metadata:findSeries", new TypeReference<GroupedQuery<FindSeries.Request>>() {
        }, new TypeReference<FindSeries>() {
        });

    public static final GrpcDescriptor<GroupedQuery<CountSeries.Request>, CountSeries>
        METADATA_COUNT_SERIES =
        descriptor("metadata:countSeries", new TypeReference<GroupedQuery<CountSeries.Request>>() {
        }, new TypeReference<CountSeries>() {
        });

    public static final GrpcDescriptor<GroupedQuery<DeleteSeries.Request>, DeleteSeries>
        METADATA_DELETE_SERIES = descriptor("metadata:deleteSeries",
        new TypeReference<GroupedQuery<DeleteSeries.Request>>() {
        }, new TypeReference<DeleteSeries>() {
        });

    public static final GrpcDescriptor<GroupedQuery<WriteMetadata.Request>, WriteMetadata>
        METADATA_WRITE = descriptor("metadata:writeSeries",
        new TypeReference<GroupedQuery<WriteMetadata.Request>>() {
        }, new TypeReference<WriteMetadata>() {
        });

    public static final GrpcDescriptor<GroupedQuery<TagKeyCount.Request>, TagKeyCount>
        SUGGEST_TAG_KEY_COUNT =
        descriptor("suggest:tagKeyCount", new TypeReference<GroupedQuery<TagKeyCount.Request>>() {
        }, new TypeReference<TagKeyCount>() {
        });

    public static final GrpcDescriptor<GroupedQuery<KeySuggest.Request>, KeySuggest> SUGGEST_KEY =
        descriptor("suggest:key", new TypeReference<GroupedQuery<KeySuggest.Request>>() {
        }, new TypeReference<KeySuggest>() {
        });

    public static final GrpcDescriptor<GroupedQuery<TagSuggest.Request>, TagSuggest> SUGGEST_TAG =
        descriptor("suggest:tag", new TypeReference<GroupedQuery<TagSuggest.Request>>() {
        }, new TypeReference<TagSuggest>() {
        });

    public static final GrpcDescriptor<GroupedQuery<TagValuesSuggest.Request>, TagValuesSuggest>
        SUGGEST_TAG_VALUES = descriptor("suggest:tagValues",
        new TypeReference<GroupedQuery<TagValuesSuggest.Request>>() {
        }, new TypeReference<TagValuesSuggest>() {
        });

    public static final GrpcDescriptor<GroupedQuery<TagValueSuggest.Request>, TagValueSuggest>
        SUGGEST_TAG_VALUE =
        descriptor("suggest:tagValue", new TypeReference<GroupedQuery<TagValueSuggest.Request>>() {
        }, new TypeReference<TagValueSuggest>() {
        });
}
