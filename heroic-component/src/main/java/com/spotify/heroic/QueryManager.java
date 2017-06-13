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

package com.spotify.heroic;

import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.UsableGroupManager;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metadata.WriteMetadata;
import com.spotify.heroic.metric.QueryResult;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.querylogging.QueryContext;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import eu.toolchain.async.AsyncFuture;

import java.util.List;

public interface QueryManager extends UsableGroupManager<QueryManager.Group> {
    QueryBuilder newQueryFromString(String query);

    interface Group {
        AsyncFuture<QueryResult> query(Query query, QueryContext queryContext);

        AsyncFuture<FindTags> findTags(final FindTags.Request request);

        AsyncFuture<FindKeys> findKeys(final FindKeys.Request request);

        AsyncFuture<FindSeries> findSeries(final FindSeries.Request request);

        AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request);

        AsyncFuture<CountSeries> countSeries(final CountSeries.Request request);

        AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request);

        AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request);

        AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request);

        AsyncFuture<TagValuesSuggest> tagValuesSuggest(
            final TagValuesSuggest.Request request
        );

        AsyncFuture<TagValueSuggest> tagValueSuggest(final TagValueSuggest.Request request);

        AsyncFuture<WriteMetadata> writeSeries(final WriteMetadata.Request request);

        AsyncFuture<WriteMetric> writeMetric(final WriteMetric.Request write);

        List<ClusterShard> shards();
    }
}
