/*
 * Copyright (c) 2016 Spotify AB.
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
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.metric.FullQuery;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import eu.toolchain.async.AsyncFuture;
import java.util.Optional;
import lombok.RequiredArgsConstructor;

public class TracingClusterNode implements ClusterNode {
    private final ClusterNode delegateNode;
    private final QueryTrace.Identifier queryIdentifier;

    public TracingClusterNode(final ClusterNode delegate, final QueryTrace.Identifier identifier) {
        this.delegateNode = delegate;
        this.queryIdentifier = QueryTrace.identifier(identifier + "#query");
    }

    @Override
    public NodeMetadata metadata() {
        return delegateNode.metadata();
    }

    @Override
    public AsyncFuture<NodeMetadata> fetchMetadata() {
        return delegateNode.fetchMetadata();
    }

    @Override
    public AsyncFuture<Void> close() {
        return delegateNode.close();
    }

    @Override
    public ClusterNode.Group useOptionalGroup(final Optional<String> group) {
        return new Group(delegateNode.useOptionalGroup(group));
    }

    @RequiredArgsConstructor
    public class Group implements ClusterNode.Group {
        private final ClusterNode.Group delegateGroup;

        @Override
        public ClusterNode node() {
            return delegateGroup.node();
        }

        @Override
        public AsyncFuture<FullQuery> query(FullQuery.Request request) {
            return delegateGroup.query(request).directTransform(FullQuery.trace(queryIdentifier));
        }

        @Override
        public AsyncFuture<FindTags> findTags(FindTags.Request request) {
            return delegateGroup.findTags(request);
        }

        @Override
        public AsyncFuture<FindKeys> findKeys(FindKeys.Request request) {
            return delegateGroup.findKeys(request);
        }

        @Override
        public AsyncFuture<FindSeries> findSeries(FindSeries.Request request) {
            return delegateGroup.findSeries(request);
        }

        @Override
        public AsyncFuture<DeleteSeries> deleteSeries(DeleteSeries.Request request) {
            return delegateGroup.deleteSeries(request);
        }

        @Override
        public AsyncFuture<CountSeries> countSeries(CountSeries.Request request) {
            return delegateGroup.countSeries(request);
        }

        @Override
        public AsyncFuture<TagKeyCount> tagKeyCount(TagKeyCount.Request request) {
            return delegateGroup.tagKeyCount(request);
        }

        @Override
        public AsyncFuture<TagSuggest> tagSuggest(TagSuggest.Request request) {
            return delegateGroup.tagSuggest(request);
        }

        @Override
        public AsyncFuture<KeySuggest> keySuggest(KeySuggest.Request request) {
            return delegateGroup.keySuggest(request);
        }

        @Override
        public AsyncFuture<TagValuesSuggest> tagValuesSuggest(TagValuesSuggest.Request request) {
            return delegateGroup.tagValuesSuggest(request);
        }

        @Override
        public AsyncFuture<TagValueSuggest> tagValueSuggest(TagValueSuggest.Request request) {
            return delegateGroup.tagValueSuggest(request);
        }

        @Override
        public AsyncFuture<WriteMetadata> writeSeries(final WriteMetadata.Request request) {
            return delegateGroup.writeSeries(request);
        }

        @Override
        public AsyncFuture<WriteMetric> writeMetric(final WriteMetric.Request request) {
            return delegateGroup.writeMetric(request);
        }

        public String toString() {
            return queryIdentifier.toString();
        }
    }
}
