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
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.metadata.CountSeries;
import com.spotify.heroic.metadata.DeleteSeries;
import com.spotify.heroic.metadata.FindKeys;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.FindTags;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.ResultGroups;
import com.spotify.heroic.metric.WriteMetric;
import com.spotify.heroic.metric.WriteResult;
import com.spotify.heroic.suggest.KeySuggest;
import com.spotify.heroic.suggest.TagKeyCount;
import com.spotify.heroic.suggest.TagSuggest;
import com.spotify.heroic.suggest.TagValueSuggest;
import com.spotify.heroic.suggest.TagValuesSuggest;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Collector;
import eu.toolchain.async.Transform;
import lombok.RequiredArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

@RequiredArgsConstructor
public class CoreClusterNodeGroup implements ClusterNodeGroup {
    public static final QueryTrace.Identifier QUERY_NODE =
        QueryTrace.identifier(CoreClusterNodeGroup.class, "query_node");
    public static final QueryTrace.Identifier QUERY =
        QueryTrace.identifier(CoreClusterNodeGroup.class, "query");

    private final AsyncFramework async;
    private final List<ClusterShardGroup> entries;

    @Override
    public List<ClusterShardGroup> shards() {
        return entries;
    }

    @Override
    public ClusterNode node() {
        throw new IllegalStateException("No node associated with ClusterNodeGroups");
    }

    @Override
    public AsyncFuture<ResultGroups> query(
        MetricType source, Filter filter, DateRange range, AggregationInstance aggregation,
        QueryOptions options
    ) {
        return run(g -> g.query(source, filter, range, aggregation, options),
            c -> ResultGroups.shardError(QUERY_NODE, c), ResultGroups.collect(QUERY));
    }

    @Override
    public AsyncFuture<FindTags> findTags(final FindTags.Request request) {
        return run(g -> g.findTags(request), FindTags::shardError, FindTags.reduce());
    }

    @Override
    public AsyncFuture<FindKeys> findKeys(final FindKeys.Request request) {
        return run(g -> g.findKeys(request), FindKeys::shardError, FindKeys.reduce());
    }

    @Override
    public AsyncFuture<FindSeries> findSeries(final FindSeries.Request request) {
        return run(g -> g.findSeries(request), FindSeries::shardError,
            FindSeries.reduce(request.getLimit()));
    }

    @Override
    public AsyncFuture<DeleteSeries> deleteSeries(final DeleteSeries.Request request) {
        return run(g -> g.deleteSeries(request), DeleteSeries::shardError, DeleteSeries.reduce());
    }

    @Override
    public AsyncFuture<CountSeries> countSeries(final CountSeries.Request request) {
        return run(g -> g.countSeries(request), CountSeries::shardError, CountSeries.reduce());
    }

    @Override
    public AsyncFuture<TagKeyCount> tagKeyCount(final TagKeyCount.Request request) {
        return run(g -> g.tagKeyCount(request), TagKeyCount::shardError,
            TagKeyCount.reduce(request.getLimit()));
    }

    @Override
    public AsyncFuture<TagSuggest> tagSuggest(final TagSuggest.Request request) {
        return run(g -> g.tagSuggest(request), TagSuggest::shardError,
            TagSuggest.reduce(request.getLimit()));
    }

    @Override
    public AsyncFuture<KeySuggest> keySuggest(final KeySuggest.Request request) {
        return run(g -> g.keySuggest(request), KeySuggest::shardError,
            KeySuggest.reduce(request.getLimit()));
    }

    @Override
    public AsyncFuture<TagValuesSuggest> tagValuesSuggest(final TagValuesSuggest.Request request) {
        return run(g -> g.tagValuesSuggest(request), TagValuesSuggest::shardError,
            TagValuesSuggest.reduce(request.getLimit(), request.getGroupLimit()));
    }

    @Override
    public AsyncFuture<TagValueSuggest> tagValueSuggest(final TagValueSuggest.Request request) {
        return run(g -> g.tagValueSuggest(request), TagValueSuggest::shardError,
            TagValueSuggest.reduce(request.getLimit()));
    }

    @Override
    public AsyncFuture<WriteResult> writeSeries(DateRange range, Series series) {
        return run(g -> g.writeSeries(range, series), WriteResult::shardError,
            WriteResult.reduce());
    }

    @Override
    public AsyncFuture<WriteResult> writeMetric(WriteMetric write) {
        return run(g -> g.writeMetric(write), WriteResult::shardError, WriteResult.reduce());
    }

    private <T> AsyncFuture<T> run(
        final Function<ClusterNode.Group, AsyncFuture<T>> function,
        final Function<ClusterShardGroup, Transform<Throwable, T>> catcher,
        final Collector<T, T> collector
    ) {
        final List<AsyncFuture<T>> futures = new ArrayList<>(entries.size());

        for (final ClusterShardGroup shard : entries) {
            futures.add(shard.apply(g -> function.apply(g)).catchFailed(catcher.apply(shard)));
        }

        return async.collect(futures, collector);
    }
}
