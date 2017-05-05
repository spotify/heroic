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
import eu.toolchain.async.AsyncFuture;

public interface ClusterNode extends UsableGroupManager<ClusterNode.Group> {
    NodeMetadata metadata();

    AsyncFuture<NodeMetadata> fetchMetadata();

    AsyncFuture<Void> close();

    /**
     * Perform a check to see if this node connection should be considered alive or not.
     *
     * @return {@code true} if this node should be used for requests, {@code false} otherwise.
     */
    default boolean isAlive() {
        return true;
    }

    interface Group {
        ClusterNode node();

        /**
         * Perform a simple ping to check that the remote end responds.
         */
        AsyncFuture<Void> ping();

        AsyncFuture<FullQuery> query(FullQuery.Request request);

        AsyncFuture<FindTags> findTags(FindTags.Request request);

        AsyncFuture<FindKeys> findKeys(FindKeys.Request request);

        AsyncFuture<FindSeries> findSeries(FindSeries.Request request);

        AsyncFuture<DeleteSeries> deleteSeries(DeleteSeries.Request request);

        AsyncFuture<CountSeries> countSeries(CountSeries.Request request);

        AsyncFuture<TagKeyCount> tagKeyCount(TagKeyCount.Request request);

        AsyncFuture<TagSuggest> tagSuggest(TagSuggest.Request request);

        AsyncFuture<KeySuggest> keySuggest(KeySuggest.Request request);

        AsyncFuture<TagValuesSuggest> tagValuesSuggest(TagValuesSuggest.Request request);

        AsyncFuture<TagValueSuggest> tagValueSuggest(TagValueSuggest.Request request);

        AsyncFuture<WriteMetadata> writeSeries(WriteMetadata.Request request);

        AsyncFuture<WriteMetric> writeMetric(WriteMetric.Request request);
    }
}
