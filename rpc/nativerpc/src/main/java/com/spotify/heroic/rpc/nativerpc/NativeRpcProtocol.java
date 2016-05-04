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
import java.util.List;
import java.util.Optional;

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
        public AsyncFuture<Void> close() {
            return async.resolved(null);
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
                return NativeRpcClusterNode.this;
            }

            @Override
            public AsyncFuture<ResultGroups> query(
                MetricType source, Filter filter, DateRange range, AggregationInstance aggregation,
                QueryOptions options
            ) {
                return request(METRICS_QUERY,
                    new RpcQuery(source, filter, range, aggregation, options), ResultGroups.class);
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
                return request(METADATA_WRITE, new RpcWriteSeries(range, series),
                    WriteResult.class);
            }

            @Override
            public AsyncFuture<TagKeyCount> tagKeyCount(RangeFilter filter) {
                return request(SUGGEST_TAG_KEY_COUNT, filter, TagKeyCount.class);
            }

            @Override
            public AsyncFuture<TagSuggest> tagSuggest(
                RangeFilter filter, MatchOptions match, Optional<String> key, Optional<String> value
            ) {
                return request(SUGGEST_TAG, new RpcTagSuggest(filter, match, key, value),
                    TagSuggest.class);
            }

            @Override
            public AsyncFuture<KeySuggest> keySuggest(
                RangeFilter filter, MatchOptions match, Optional<String> key
            ) {
                return request(SUGGEST_KEY, new RpcKeySuggest(filter, match, key),
                    KeySuggest.class);
            }

            @Override
            public AsyncFuture<TagValuesSuggest> tagValuesSuggest(
                RangeFilter filter, List<String> exclude, int groupLimit
            ) {
                return request(SUGGEST_TAG_VALUES,
                    new RpcSuggestTagValues(filter, exclude, groupLimit), TagValuesSuggest.class);
            }

            @Override
            public AsyncFuture<TagValueSuggest> tagValueSuggest(
                RangeFilter filter, Optional<String> key
            ) {
                return request(SUGGEST_TAG_VALUE, new RpcSuggestTagValue(filter, key),
                    TagValueSuggest.class);
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
    }

    @Data
    public static class RpcQuery {
        private final MetricType source;
        private final Filter filter;
        private final DateRange range;
        private final AggregationInstance aggregation;
        private final QueryOptions options;

        @JsonCreator
        public RpcQuery(
            @JsonProperty("source") final MetricType source,
            @JsonProperty("filter") final Filter filter,
            @JsonProperty("range") final DateRange range,
            @JsonProperty("aggregation") final AggregationInstance aggregation,
            @JsonProperty("options") final QueryOptions options
        ) {
            this.source = checkNotNull(source, "source");
            this.filter = checkNotNull(filter, "filter");
            this.range = checkNotNull(range, "range");
            this.aggregation = checkNotNull(aggregation, "aggregation");
            this.options = checkNotNull(options, "options");
        }
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
}
