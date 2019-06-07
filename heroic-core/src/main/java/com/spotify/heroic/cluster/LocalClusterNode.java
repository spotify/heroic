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

import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.MetadataBackend;
import com.spotify.heroic.metadata.MetadataManager;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.MetricBackendGroup;
import com.spotify.heroic.metric.MetricManager;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.SuggestBackend;
import com.spotify.heroic.suggest.SuggestManager;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import java.util.Optional;
import javax.inject.Inject;

@ClusterScope
public class LocalClusterNode implements ClusterNode {
    private final AsyncFramework async;
    private final NodeMetadata localMetadata;
    private final MetricManager metrics;
    private final MetadataManager metadata;
    private final SuggestManager suggest;

    @Inject
    public LocalClusterNode(
        AsyncFramework async, NodeMetadata localMetadata, MetricManager metrics,
        MetadataManager metadata, SuggestManager suggest
    ) {
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
    public AsyncFuture<NodeMetadata> fetchMetadata() {
        return async.resolved(localMetadata);
    }

    @Override
    public AsyncFuture<Void> close() {
        return async.resolved(null);
    }

    @Override
    public Group useOptionalGroup(final Optional<String> group) {
        return new LocalGroup(group);
    }

    public String toString() {
        return "LocalClusterNode(localMetadata=" + this.localMetadata + ")";
    }

    private final class LocalGroup implements Group {
        private final Optional<String> group;

        @java.beans.ConstructorProperties({ "group" })
        public LocalGroup(final Optional<String> group) {
            this.group = group;
        }

        @Override
        public ClusterNode node() {
            return LocalClusterNode.this;
        }

        @Override
        public AsyncFuture<Void> ping() {
            return async.resolved();
        }

        @Override
        public AsyncFuture<FullQuery> query(final FullQuery.Request request) {
            return metrics().query(request);
        }

        @Override
        public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
            return metadata().findTags(request);
        }

        @Override
        public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
            return metadata().findKeys(request);
        }

        @Override
        public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
            return metadata().findSeries(request);
        }

        @Override
        public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
            return metadata().deleteSeries(request);
        }

        @Override
        public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
            return metadata().countSeries(request);
        }

        @Override
        public AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request) {
            return suggest().tagKeyCount(request);
        }

        @Override
        public AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request) {
            return suggest().tagSuggest(request);
        }

        @Override
        public AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request) {
            return suggest().keySuggest(request);
        }

        @Override
        public AsyncFuture<TagValuesSuggest> tagValuesSuggest(
            final TagValuesSuggest.Request request
        ) {
            return suggest().tagValuesSuggest(request);
        }

        @Override
        public AsyncFuture<TagValueSuggest> tagValueSuggest(final TagValueSuggest.Request request) {
            return suggest().tagValueSuggest(request);
        }

        @Override
        public AsyncFuture<WriteMetadata> writeSeries(final WriteMetadata.Request request) {
            return metadata().write(request);
        }

        @Override
        public AsyncFuture<WriteMetric> writeMetric(final WriteMetric.Request request) {
            return metrics().write(request);
        }

        private SuggestBackend suggest() {
            return suggest.useOptionalGroup(group);
        }

        private MetadataBackend metadata() {
            return metadata.useOptionalGroup(group);
        }

        private MetricBackendGroup metrics() {
            return metrics.useOptionalGroup(group);
        }
    }
}
