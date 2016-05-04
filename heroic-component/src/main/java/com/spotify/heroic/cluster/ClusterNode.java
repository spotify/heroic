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

import com.spotify.heroic.QueryOptions;
import com.spotify.heroic.aggregation.AggregationInstance;
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
import eu.toolchain.async.AsyncFuture;

import java.util.List;
import java.util.Optional;

public interface ClusterNode {
    NodeMetadata metadata();

    AsyncFuture<Void> close();

    Group useGroup(String group);

    interface Group {
        ClusterNode node();

        AsyncFuture<ResultGroups> query(
            MetricType source, Filter filter, DateRange range, AggregationInstance aggregation,
            QueryOptions options
        );

        AsyncFuture<FindTags> findTags(RangeFilter filter);

        AsyncFuture<FindKeys> findKeys(RangeFilter filter);

        AsyncFuture<FindSeries> findSeries(RangeFilter filter);

        AsyncFuture<DeleteSeries> deleteSeries(RangeFilter filter);

        AsyncFuture<CountSeries> countSeries(RangeFilter filter);

        AsyncFuture<TagKeyCount> tagKeyCount(RangeFilter filter);

        AsyncFuture<TagSuggest> tagSuggest(
            RangeFilter filter, MatchOptions options, Optional<String> key, Optional<String> value
        );

        AsyncFuture<KeySuggest> keySuggest(
            RangeFilter filter, MatchOptions options, Optional<String> key
        );

        AsyncFuture<TagValuesSuggest> tagValuesSuggest(
            RangeFilter filter, List<String> exclude, int groupLimit
        );

        AsyncFuture<TagValueSuggest> tagValueSuggest(RangeFilter filter, Optional<String> key);

        AsyncFuture<WriteResult> writeSeries(DateRange range, Series series);

        AsyncFuture<WriteResult> writeMetric(WriteMetric write);
    }
}
