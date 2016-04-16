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
import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.cluster.RpcProtocol;
import com.spotify.heroic.cluster.TracingClusterNodeGroup;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.ResultGroups;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Managed;
import eu.toolchain.async.ManagedSetup;
import eu.toolchain.async.ResolvableFuture;
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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

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
                    value.shutdown();
                    value.awaitTermination(10, TimeUnit.SECONDS);
                    return null;
                });
            }
        });

        return channel.start().lazyTransform(n -> {
            final GrpcRpcClient client = new GrpcRpcClient(async, address, mapper, channel);

            return client
                .request(METADATA)
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
        public AsyncFuture<Void> close() {
            return client.close();
        }

        @Override
        public ClusterNode.Group useGroup(String group) {
            return new TracingClusterNodeGroup(uri.toString(),
                new Group(Optional.ofNullable(group)));
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
            public AsyncFuture<ResultGroups> query(
                MetricType source, Filter filter, DateRange range, AggregationInstance aggregation,
                QueryOptions options
            ) {
                return request(METRICS_QUERY,
                    new RpcFullQuery(source, filter, range, aggregation, options));
            }

            @Override
            public AsyncFuture<WriteResult> writeMetric(WriteMetric write) {
                return request(METRICS_WRITE, write);
            }

            @Override
            public AsyncFuture<FindTags> findTags(RangeFilter filter) {
                return request(METADATA_FIND_TAGS, filter);
            }

            @Override
            public AsyncFuture<FindKeys> findKeys(RangeFilter filter) {
                return request(METADATA_FIND_KEYS, filter);
            }

            @Override
            public AsyncFuture<FindSeries> findSeries(RangeFilter filter) {
                return request(METADATA_FIND_SERIES, filter);
            }

            @Override
            public AsyncFuture<CountSeries> countSeries(RangeFilter filter) {
                return request(METADATA_COUNT_SERIES, filter);
            }

            @Override
            public AsyncFuture<DeleteSeries> deleteSeries(RangeFilter filter) {
                return request(METADATA_DELETE_SERIES, filter);
            }

            @Override
            public AsyncFuture<WriteResult> writeSeries(DateRange range, Series series) {
                return request(METADATA_WRITE, new RpcWriteSeries(range, series));
            }

            @Override
            public AsyncFuture<TagKeyCount> tagKeyCount(RangeFilter filter) {
                return request(SUGGEST_TAG_KEY_COUNT, filter);
            }

            @Override
            public AsyncFuture<TagSuggest> tagSuggest(
                RangeFilter filter, MatchOptions match, Optional<String> key, Optional<String> value
            ) {
                return request(SUGGEST_TAG, new RpcTagSuggest(filter, match, key, value));
            }

            @Override
            public AsyncFuture<KeySuggest> keySuggest(
                RangeFilter filter, MatchOptions match, Optional<String> key
            ) {
                return request(SUGGEST_KEY, new RpcKeySuggest(filter, match, key));
            }

            @Override
            public AsyncFuture<TagValuesSuggest> tagValuesSuggest(
                RangeFilter filter, List<String> exclude, int groupLimit
            ) {
                return request(SUGGEST_TAG_VALUES,
                    new RpcSuggestTagValues(filter, exclude, groupLimit));
            }

            @Override
            public AsyncFuture<TagValueSuggest> tagValueSuggest(
                RangeFilter filter, Optional<String> key
            ) {
                return request(SUGGEST_TAG_VALUE, new RpcSuggestTagValue(filter, key));
            }

            private <T, R> AsyncFuture<R> request(
                GrpcEndpointSpecification<GroupedQuery<T>, R> endpoint, T body
            ) {
                final GroupedQuery<T> grouped = new GroupedQuery<>(group, body);
                return client.request(endpoint, grouped);
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
    }

    @Data
    public static class RpcFullQuery {
        private final MetricType source;
        private final Filter filter;
        private final DateRange range;
        private final AggregationInstance aggregation;
        private final QueryOptions options;
    }

    @Data
    public static class RpcTagSuggest {
        private final RangeFilter filter;
        private final MatchOptions match;
        private final Optional<String> key;
        private final Optional<String> value;

        public RpcTagSuggest(
            @JsonProperty("range") final RangeFilter filter,
            @JsonProperty("match") final MatchOptions match,
            @JsonProperty("key") final Optional<String> key,
            @JsonProperty("value") final Optional<String> value
        ) {
            this.filter = filter;
            this.match = checkNotNull(match, "match");
            this.key = key;
            this.value = value;
        }
    }

    @Data
    public static class RpcSuggestTagValues {
        private final RangeFilter filter;
        private final List<String> exclude;
        private final int groupLimit;

        public RpcSuggestTagValues(
            @JsonProperty("filter") final RangeFilter filter,
            @JsonProperty("exclude") final List<String> exclude,
            @JsonProperty("groupLimit") final Integer groupLimit
        ) {
            this.filter = filter;
            this.exclude = checkNotNull(exclude, "exclude");
            this.groupLimit = checkNotNull(groupLimit, "groupLimit");
        }
    }

    @Data
    public static class RpcSuggestTagValue {
        private final RangeFilter filter;
        private final Optional<String> key;

        public RpcSuggestTagValue(
            @JsonProperty("filter") final RangeFilter filter,
            @JsonProperty("key") final Optional<String> key
        ) {
            this.filter = checkNotNull(filter, "filter");
            this.key = key;
        }
    }

    @Data
    public static class RpcKeySuggest {
        private final RangeFilter filter;
        private final MatchOptions match;
        private final Optional<String> key;

        public RpcKeySuggest(
            @JsonProperty("filter") final RangeFilter filter,
            @JsonProperty("match") final MatchOptions match,
            @JsonProperty("key") final Optional<String> key
        ) {
            this.filter = checkNotNull(filter, "filter");
            this.match = checkNotNull(match, "match");
            this.key = key;
        }
    }

    @Data
    public static class RpcWriteSeries {
        private final DateRange range;
        private final Series series;

        public RpcWriteSeries(
            @JsonProperty("range") final DateRange range,
            @JsonProperty("series") final Series series
        ) {
            this.range = checkNotNull(range);
            this.series = checkNotNull(series);
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

    private static <Q, R> GrpcEndpointSpecification<Q, R> descriptor(
        final String endpointName, final TypeReference<Q> requestType,
        final TypeReference<R> responseType
    ) {
        final MethodDescriptor<byte[], byte[]> descriptor =
            MethodDescriptor.create(MethodDescriptor.MethodType.SERVER_STREAMING,
                generateFullMethodName(SERVICE, endpointName), BYTE_MARSHALLER, BYTE_MARSHALLER);

        return new GrpcRpcEndpointSpec<>(requestType, responseType, descriptor);
    }

    public static final GrpcEndpointSpecification<GrpcRpcEmptyBody, NodeMetadata> METADATA =
        descriptor("metadata", new TypeReference<GrpcRpcEmptyBody>() {
        }, new TypeReference<NodeMetadata>() {
        });

    public static final GrpcEndpointSpecification<GroupedQuery<RpcFullQuery>, ResultGroups>
        METRICS_QUERY =
        descriptor("metrics:query", new TypeReference<GroupedQuery<RpcFullQuery>>() {
        }, new TypeReference<ResultGroups>() {
        });

    public static final GrpcEndpointSpecification<GroupedQuery<WriteMetric>, WriteResult>
        METRICS_WRITE = descriptor("metrics:write", new TypeReference<GroupedQuery<WriteMetric>>() {
    }, new TypeReference<WriteResult>() {
    });

    public static final GrpcEndpointSpecification<GroupedQuery<RangeFilter>, FindTags>
        METADATA_FIND_TAGS =
        descriptor("metadata:findTags", new TypeReference<GroupedQuery<RangeFilter>>() {
        }, new TypeReference<FindTags>() {
        });

    public static final GrpcEndpointSpecification<GroupedQuery<RangeFilter>, FindKeys>
        METADATA_FIND_KEYS =
        descriptor("metadata:findKeys", new TypeReference<GroupedQuery<RangeFilter>>() {
        }, new TypeReference<FindKeys>() {
        });

    public static final GrpcEndpointSpecification<GroupedQuery<RangeFilter>, FindSeries>
        METADATA_FIND_SERIES =
        descriptor("metadata:findSeries", new TypeReference<GroupedQuery<RangeFilter>>() {
        }, new TypeReference<FindSeries>() {
        });

    public static final GrpcEndpointSpecification<GroupedQuery<RangeFilter>, CountSeries>
        METADATA_COUNT_SERIES =
        descriptor("metadata:countSeries", new TypeReference<GroupedQuery<RangeFilter>>() {
        }, new TypeReference<CountSeries>() {
        });

    public static final GrpcEndpointSpecification<GroupedQuery<RangeFilter>, DeleteSeries>
        METADATA_DELETE_SERIES =
        descriptor("metadata:deleteSeries", new TypeReference<GroupedQuery<RangeFilter>>() {
        }, new TypeReference<DeleteSeries>() {
        });

    public static final GrpcEndpointSpecification<GroupedQuery<RpcWriteSeries>, WriteResult>
        METADATA_WRITE =
        descriptor("metadata:writeSeries", new TypeReference<GroupedQuery<RpcWriteSeries>>() {
        }, new TypeReference<WriteResult>() {
        });

    public static final GrpcEndpointSpecification<GroupedQuery<RangeFilter>, TagKeyCount>
        SUGGEST_TAG_KEY_COUNT =
        descriptor("suggest:tagKeyCount", new TypeReference<GroupedQuery<RangeFilter>>() {
        }, new TypeReference<TagKeyCount>() {
        });

    public static final GrpcEndpointSpecification<GroupedQuery<RpcKeySuggest>, KeySuggest>
        SUGGEST_KEY = descriptor("suggest:key", new TypeReference<GroupedQuery<RpcKeySuggest>>() {
    }, new TypeReference<KeySuggest>() {
    });

    public static final GrpcEndpointSpecification<GroupedQuery<RpcTagSuggest>, TagSuggest>
        SUGGEST_TAG = descriptor("suggest:tag", new TypeReference<GroupedQuery<RpcTagSuggest>>() {
    }, new TypeReference<TagSuggest>() {
    });

    public static final GrpcEndpointSpecification<GroupedQuery<RpcSuggestTagValues>,
        TagValuesSuggest>
        SUGGEST_TAG_VALUES =
        descriptor("suggest:tagValues", new TypeReference<GroupedQuery<RpcSuggestTagValues>>() {
        }, new TypeReference<TagValuesSuggest>() {
        });

    public static final GrpcEndpointSpecification<GroupedQuery<RpcSuggestTagValue>, TagValueSuggest>
        SUGGEST_TAG_VALUE =
        descriptor("suggest:tagValue", new TypeReference<GroupedQuery<RpcSuggestTagValue>>() {
        }, new TypeReference<TagValueSuggest>() {
        });
}
