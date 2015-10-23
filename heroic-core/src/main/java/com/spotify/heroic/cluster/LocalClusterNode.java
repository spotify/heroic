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

package com.spotify.heroic.cluster;

import java.util.List;

import com.google.inject.Inject;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.common.BackendGroupException;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.RangeFilter;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryOptions;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.ResultGroups;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.MatchOptions;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

@ToString(exclude = { "async", "metrics", "metadata", "suggest" })
public class LocalClusterNode implements ClusterNode {
    private final AsyncFramework async;
    private final NodeMetadata localMetadata;
    private final MetricManager metrics;
    private final MetadataManager metadata;
    private final SuggestManager suggest;

    @Inject
    public LocalClusterNode(AsyncFramework async, NodeMetadata localMetadata, MetricManager metrics,
            MetadataManager metadata, SuggestManager suggest) {
        this.async = async;
        this.localMetadata = localMetadata;
        this.metrics = metrics;
        this.metadata = metadata;
        this.suggest = suggest;
    }

    @Override
    public NodeMetadata metadata() {
        return localMetadata;
    }

    @Override
    public AsyncFuture<Void> close() {
        return async.resolved(null);
    }

    @Override
    public Group useGroup(String group) {
        return new TracingClusterNodeGroup(LocalClusterNode.class, new LocalGroup(group));
    }

    @RequiredArgsConstructor
    private final class LocalGroup implements ClusterNode.Group {
        private final String group;

        @Override
        public ClusterNode node() {
            return LocalClusterNode.this;
        }

        @Override
        public AsyncFuture<ResultGroups> query(MetricType source, Filter filter,
                DateRange range, Aggregation aggregation, QueryOptions options) {
            return metrics().query(source, filter, range, aggregation, options);
        }

        @Override
        public AsyncFuture<FindTags> findTags(RangeFilter filter) {
            return metadata().findTags(filter);
        }

        @Override
        public AsyncFuture<FindKeys> findKeys(RangeFilter filter) {
            return metadata().findKeys(filter);
        }

        @Override
        public AsyncFuture<FindSeries> findSeries(RangeFilter filter) {
            return metadata().findSeries(filter);
        }

        @Override
        public AsyncFuture<DeleteSeries> deleteSeries(RangeFilter filter) {
            return metadata().deleteSeries(filter);
        }

        @Override
        public AsyncFuture<CountSeries> countSeries(RangeFilter filter) {
            return metadata().countSeries(filter);
        }

        @Override
        public AsyncFuture<TagKeyCount> tagKeyCount(RangeFilter filter) {
            return suggest().tagKeyCount(filter);
        }

        @Override
        public AsyncFuture<TagSuggest> tagSuggest(RangeFilter filter, MatchOptions options, String key, String value) {
            return suggest().tagSuggest(filter, options, key, value);
        }

        @Override
        public AsyncFuture<KeySuggest> keySuggest(RangeFilter filter, MatchOptions options, String key) {
            return suggest().keySuggest(filter, options, key);
        }

        @Override
        public AsyncFuture<TagValuesSuggest> tagValuesSuggest(RangeFilter filter, List<String> exclude, int groupLimit) {
            return suggest().tagValuesSuggest(filter, exclude, groupLimit);
        }

        @Override
        public AsyncFuture<TagValueSuggest> tagValueSuggest(RangeFilter filter, String key) {
            return suggest().tagValueSuggest(filter, key);
        }

        @Override
        public AsyncFuture<WriteResult> writeSeries(DateRange range, Series series) {
            return metadata().write(series, range);
        }

        @Override
        public AsyncFuture<WriteResult> writeMetric(WriteMetric write) {
            return metrics().write(write);
        }

        private SuggestBackend suggest() {
            try {
                return suggest.useGroup(group);
            } catch (BackendGroupException e) {
                throw new IllegalArgumentException("invalid group: " + group, e);
            }
        }

        private MetadataBackend metadata() {
            try {
                return metadata.useGroup(group);
            } catch (BackendGroupException e) {
                throw new IllegalArgumentException("invalid group: " + group, e);
            }
        }

        private MetricBackendGroup metrics() {
            try {
                return metrics.useGroup(group);
            } catch (BackendGroupException e) {
                throw new IllegalArgumentException("invalid group: " + group, e);
            }
        }
    }
}