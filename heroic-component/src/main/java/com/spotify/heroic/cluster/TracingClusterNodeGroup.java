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

public class TracingClusterNodeGroup implements ClusterNode.Group {
    private final ClusterNode.Group delegate;
    private final QueryTrace.Identifier query;

    public TracingClusterNodeGroup(final Class<?> where, final ClusterNode.Group delegate) {
        this(where.getCanonicalName(), delegate);
    }

    public TracingClusterNodeGroup(final String identifier, final ClusterNode.Group delegate) {
        this.delegate = delegate;
        this.query = QueryTrace.identifier(identifier + "#query");
    }

    @Override
    public ClusterNode node() {
        return delegate.node();
    }

    @Override
    public AsyncFuture<FullQuery> query(FullQuery.Request request) {
        return delegate.query(request).directTransform(FullQuery.trace(query));
    }

    @Override
    public AsyncFuture<FindTags> findTags(FindTags.Request request) {
        return delegate.findTags(request);
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(FindKeys.Request request) {
        return delegate.findKeys(request);
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(FindSeries.Request request) {
        return delegate.findSeries(request);
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(DeleteSeries.Request request) {
        return delegate.deleteSeries(request);
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(CountSeries.Request request) {
        return delegate.countSeries(request);
    }

    @Override
    public AsyncFuture<TagKeyCount> tagKeyCount(TagKeyCount.Request request) {
        return delegate.tagKeyCount(request);
    }

    @Override
    public AsyncFuture<TagSuggest> tagSuggest(TagSuggest.Request request) {
        return delegate.tagSuggest(request);
    }

    @Override
    public AsyncFuture<KeySuggest> keySuggest(KeySuggest.Request request) {
        return delegate.keySuggest(request);
    }

    @Override
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(TagValuesSuggest.Request request) {
        return delegate.tagValuesSuggest(request);
    }

    @Override
    public AsyncFuture<TagValueSuggest> tagValueSuggest(TagValueSuggest.Request request) {
        return delegate.tagValueSuggest(request);
    }

    @Override
    public AsyncFuture<WriteMetadata> writeSeries(final WriteMetadata.Request request) {
        return delegate.writeSeries(request);
    }

    @Override
    public AsyncFuture<WriteMetric> writeMetric(final WriteMetric.Request request) {
        return delegate.writeMetric(request);
    }
}
