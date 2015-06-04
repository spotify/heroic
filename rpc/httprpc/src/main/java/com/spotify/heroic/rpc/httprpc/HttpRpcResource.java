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

package com.spotify.heroic.rpc.httprpc;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.URI;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.inject.Inject;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.cluster.ClusterNode;
import com.spotify.heroic.cluster.model.NodeMetadata;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.httpclient.HttpClientSession;
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
import com.spotify.heroic.rpc.httprpc.model.RpcGroupedQuery;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.model.KeySuggest;
import com.spotify.heroic.suggest.model.MatchOptions;
import com.spotify.heroic.suggest.model.TagKeyCount;
import com.spotify.heroic.suggest.model.TagSuggest;
import com.spotify.heroic.suggest.model.TagValueSuggest;
import com.spotify.heroic.suggest.model.TagValuesSuggest;
import com.spotify.heroic.utils.HttpAsyncUtils;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;

@Path("rpc")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class HttpRpcResource {
    private static final String METRICS_QUERY = "metrics:query";
    private static final String METRICS_WRITE = "metrics:write";
    private static final String METADATA_FIND_TAGS = "metadata:findTags";
    private static final String METADATA_FIND_KEYS = "metadata:findKeys";
    private static final String METADATA_FIND_SERIES = "metadata:findSeries";
    private static final String METADATA_COUNT_SERIES = "metadata:countSeries";
    private static final String METADATA_DELETE_SERIES = "metadata:deleteSeries";
    private static final String SUGGEST_TAG_KEY_COUNT = "suggest:tagKeyCount";
    private static final String SUGGEST_KEY = "suggest:key";
    private static final String SUGGEST_TAG = "suggest:tag";
    private static final String SUGGEST_TAG_VALUES = "suggest:tagValues";
    private static final String SUGGEST_TAG_VALUE = "suggest:tagValue";
    private static final String METADATA_WRITE = "metadata:writeSeries";

    @Inject
    private HttpAsyncUtils httpAsync;

    @Inject
    private MetricManager metrics;

    @Inject
    private MetadataManager metadata;

    @Inject
    private SuggestManager suggest;

    @Inject
    private NodeMetadata nodeMetadata;

    @GET
    @Path("metadata")
    public Response getMetadata() {
        final HttpRpcMetadata metadata = new HttpRpcMetadata(0, nodeMetadata.getId(), nodeMetadata.getTags(),
                nodeMetadata.getCapabilities());
        return Response.status(Response.Status.OK).entity(metadata).build();
    }

    @POST
    @Path(METRICS_QUERY)
    public void query(@Suspended final AsyncResponse response, RpcGroupedQuery<RpcQuery> grouped) throws Exception {
        final RpcQuery query = grouped.getQuery();
        httpAsync.handleAsyncResume(
                response,
                metrics.useGroup(grouped.getGroup()).query(query.getSource(), query.getFilter(), query.getGroupBy(),
                        query.getRange(), query.getAggregation(), query.isNoCache()));
    }

    @POST
    @Path(METRICS_WRITE)
    public void writeMetric(@Suspended final AsyncResponse response, RpcGroupedQuery<WriteMetric> grouped)
            throws Exception {
        final WriteMetric query = grouped.getQuery();
        httpAsync.handleAsyncResume(response, metrics.useGroup(grouped.getGroup()).write(query));
    }

    @POST
    @Path(METADATA_FIND_TAGS)
    public void findTags(@Suspended final AsyncResponse response, RpcGroupedQuery<RangeFilter> grouped)
            throws Exception {
        httpAsync.handleAsyncResume(response, metadata.useGroup(grouped.getGroup()).findTags(grouped.getQuery()));
    }

    @POST
    @Path(METADATA_FIND_KEYS)
    public void metadataFindKeys(@Suspended final AsyncResponse response, RpcGroupedQuery<RangeFilter> grouped)
            throws Exception {
        httpAsync.handleAsyncResume(response, metadata.useGroup(grouped.getGroup()).findKeys(grouped.getQuery()));
    }

    @POST
    @Path(METADATA_FIND_SERIES)
    public void metadataFindSeries(@Suspended final AsyncResponse response, RpcGroupedQuery<RangeFilter> grouped)
            throws Exception {
        httpAsync.handleAsyncResume(response, metadata.useGroup(grouped.getGroup()).findSeries(grouped.getQuery()));
    }

    @POST
    @Path(METADATA_COUNT_SERIES)
    public void metadataCountSeries(@Suspended final AsyncResponse response, RpcGroupedQuery<RangeFilter> grouped)
            throws Exception {
        httpAsync.handleAsyncResume(response, metadata.useGroup(grouped.getGroup()).countSeries(grouped.getQuery()));
    }

    @POST
    @Path(METADATA_WRITE)
    public void writeSeries(@Suspended final AsyncResponse response, RpcGroupedQuery<RpcWriteSeries> grouped)
            throws Exception {
        final RpcWriteSeries query = grouped.getQuery();
        httpAsync.handleAsyncResume(response,
                metadata.useGroup(grouped.getGroup()).write(query.getSeries(), query.getRange()));
    }

    @POST
    @Path(METADATA_DELETE_SERIES)
    public void metadataDeleteSeries(@Suspended final AsyncResponse response, RpcGroupedQuery<RangeFilter> grouped)
            throws Exception {
        httpAsync.handleAsyncResume(response, metadata.useGroup(grouped.getGroup()).deleteSeries(grouped.getQuery()));
    }

    @POST
    @Path(SUGGEST_TAG_KEY_COUNT)
    public void suggestTagKeyCount(@Suspended final AsyncResponse response, RpcGroupedQuery<RangeFilter> grouped)
            throws Exception {
        httpAsync.handleAsyncResume(response, suggest.useGroup(grouped.getGroup()).tagKeyCount(grouped.getQuery()));
    }

    @POST
    @Path(SUGGEST_TAG)
    public void suggestTag(@Suspended final AsyncResponse response, RpcGroupedQuery<RpcTagSuggest> grouped)
            throws Exception {
        final RpcTagSuggest query = grouped.getQuery();
        httpAsync.handleAsyncResume(
                response,
                suggest.useGroup(grouped.getGroup()).tagSuggest(query.getFilter(), query.getMatch(), query.getKey(),
                        query.getValue()));
    }

    @POST
    @Path(SUGGEST_KEY)
    public void suggestKey(@Suspended final AsyncResponse response, RpcGroupedQuery<RpcKeySuggest> grouped)
            throws Exception {
        final RpcKeySuggest query = grouped.getQuery();
        httpAsync.handleAsyncResume(response,
                suggest.useGroup(grouped.getGroup()).keySuggest(query.getFilter(), query.getMatch(), query.getKey()));
    }

    @POST
    @Path(SUGGEST_TAG_VALUES)
    public void tagValuesSuggest(@Suspended final AsyncResponse response, RpcGroupedQuery<RpcSuggestTagValues> grouped)
            throws Exception {
        final RpcSuggestTagValues query = grouped.getQuery();
        httpAsync.handleAsyncResume(
                response,
                suggest.useGroup(grouped.getGroup()).tagValuesSuggest(query.getFilter(), query.getExclude(),
                        query.getGroupLimit()));
    }

    @POST
    @Path(SUGGEST_TAG_VALUE)
    public void tagValueSuggest(@Suspended final AsyncResponse response, RpcGroupedQuery<RpcSuggestTagValue> grouped)
            throws Exception {
        final RpcSuggestTagValue query = grouped.getQuery();
        httpAsync.handleAsyncResume(response,
                suggest.useGroup(grouped.getGroup()).tagValueSuggest(query.getFilter(), query.getKey()));
    }

    @Data
    @ToString(exclude = { "client" })
    public static class HttpRpcClusterNode implements ClusterNode {
        private final AsyncFramework async;
        private final URI uri;
        private final HttpClientSession client;
        private final NodeMetadata metadata;

        @Override
        public NodeMetadata metadata() {
            return metadata;
        }

        @Override
        public AsyncFuture<Void> close() {
            return async.<Void> resolved(null);
        }

        @Override
        public Group useGroup(String group) {
            return new NodeGroup(group);
        }

        @RequiredArgsConstructor
        private class NodeGroup implements Group {
            private final String group;

            @Override
            public AsyncFuture<ResultGroups> query(Class<? extends TimeData> source, Filter filter,
                    List<String> groupBy, DateRange range, Aggregation aggregation, boolean disableCache) {
                return post(new RpcQuery(source, filter, groupBy, range, aggregation, disableCache),
                        ResultGroups.class, METRICS_QUERY);
            }

            @Override
            public AsyncFuture<WriteResult> writeMetric(WriteMetric write) {
                return post(write, WriteResult.class, METRICS_WRITE);
            }

            @Override
            public AsyncFuture<FindTags> findTags(RangeFilter filter) {
                return post(filter, FindTags.class, METADATA_FIND_TAGS);
            }

            @Override
            public AsyncFuture<FindKeys> findKeys(RangeFilter filter) {
                return post(filter, FindKeys.class, METADATA_FIND_KEYS);
            }

            @Override
            public AsyncFuture<FindSeries> findSeries(RangeFilter filter) {
                return post(filter, FindSeries.class, METADATA_FIND_SERIES);
            }

            @Override
            public AsyncFuture<CountSeries> countSeries(RangeFilter filter) {
                return post(filter, CountSeries.class, METADATA_COUNT_SERIES);
            }

            @Override
            public AsyncFuture<DeleteSeries> deleteSeries(RangeFilter filter) {
                return post(filter, DeleteSeries.class, METADATA_DELETE_SERIES);
            }

            @Override
            public AsyncFuture<WriteResult> writeSeries(DateRange range, Series series) {
                return post(new RpcWriteSeries(range, series), WriteResult.class, METADATA_WRITE);
            }

            @Override
            public AsyncFuture<TagKeyCount> tagKeyCount(RangeFilter filter) {
                return post(filter, TagKeyCount.class, SUGGEST_TAG_KEY_COUNT);
            }

            @Override
            public AsyncFuture<TagSuggest> tagSuggest(RangeFilter filter, MatchOptions match, String key, String value) {
                return post(new RpcTagSuggest(filter, match, key, value), TagSuggest.class, SUGGEST_TAG);
            }

            @Override
            public AsyncFuture<KeySuggest> keySuggest(RangeFilter filter, MatchOptions match, String key) {
                return post(new RpcKeySuggest(filter, match, key), KeySuggest.class, SUGGEST_KEY);
            }

            @Override
            public AsyncFuture<TagValuesSuggest> tagValuesSuggest(RangeFilter filter, List<String> exclude,
                    int groupLimit) {
                return post(new RpcSuggestTagValues(filter, exclude, groupLimit), TagValuesSuggest.class,
                        SUGGEST_TAG_VALUES);
            }

            @Override
            public AsyncFuture<TagValueSuggest> tagValueSuggest(RangeFilter filter, String key) {
                return post(new RpcSuggestTagValue(filter, key), TagValueSuggest.class, SUGGEST_TAG_VALUE);
            }

            private <T, R> AsyncFuture<R> post(T query, Class<R> type, String endpoint) {
                final RpcGroupedQuery<T> grouped = new RpcGroupedQuery<>(group, query);
                return client.post(grouped, type, endpoint);
            }
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
        public RpcQuery(@JsonProperty("source") Class<? extends TimeData> source,
                @JsonProperty("filter") Filter filter, @JsonProperty("groupBy") List<String> groupBy,
                @JsonProperty("range") DateRange range, @JsonProperty("aggregation") Aggregation aggregation,
                @JsonProperty("noCache") Boolean noCache) {
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

        public RpcTagSuggest(@JsonProperty("range") RangeFilter filter, @JsonProperty("match") MatchOptions match,
                @JsonProperty("key") String key, @JsonProperty("value") String value) {
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

        public RpcSuggestTagValues(@JsonProperty("range") RangeFilter filter,
                @JsonProperty("exclude") List<String> exclude, @JsonProperty("groupLimit") Integer groupLimit) {
            this.filter = filter;
            this.exclude = checkNotNull(exclude, "exclude must not be null");
            this.groupLimit = checkNotNull(groupLimit, "groupLimit must not be null");
        }
    }

    @Data
    public static class RpcSuggestTagValue {
        private final RangeFilter filter;
        private final String key;

        public RpcSuggestTagValue(@JsonProperty("range") RangeFilter filter, @JsonProperty("key") String key) {
            this.filter = filter;
            this.key = checkNotNull(key, "key must not be null");
        }
    }

    @Data
    public static class RpcKeySuggest {
        private final RangeFilter filter;
        private final MatchOptions match;
        private final String key;

        public RpcKeySuggest(@JsonProperty("range") RangeFilter filter, @JsonProperty("match") MatchOptions match,
                @JsonProperty("key") String key) {
            this.filter = filter;
            this.match = checkNotNull(match, "match options must not be null");
            this.key = key;
        }
    }

    @Data
    public static class RpcWriteSeries {
        private final DateRange range;
        private final Series series;

        public RpcWriteSeries(@JsonProperty("range") DateRange range, @JsonProperty("series") Series series) {
            this.range = checkNotNull(range);
            this.series = checkNotNull(series);
        }
    }
}