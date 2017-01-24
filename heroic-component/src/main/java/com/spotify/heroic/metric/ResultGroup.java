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

package com.spotify.heroic.metric;

import static com.google.common.base.Preconditions.checkNotNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.cluster.ClusterShard;
import com.spotify.heroic.common.Histogram;
import com.spotify.heroic.common.Series;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import lombok.Data;

@Data
public class ResultGroup {
    final Map<String, String> key;
    final Set<Series> series;
    final MetricCollection group;
    /**
     * The interval in milliseconds for which a sample can be expected. A cadence of 0 indicates
     * that this value is unknown.
     */
    final long cadence;

    @JsonCreator
    public ResultGroup(
        @JsonProperty("key") Map<String, String> key, @JsonProperty("series") Set<Series> series,
        @JsonProperty("group") MetricCollection group, @JsonProperty("cadence") Long cadence
    ) {
        this.key = checkNotNull(key, "key");
        this.series = checkNotNull(series, "series");
        this.group = checkNotNull(group, "group");
        this.cadence = checkNotNull(cadence, "cadence");
    }

    public static Function<? super ResultGroup, ? extends ShardedResultGroup> toShardedResultGroup(
        final ClusterShard shard
    ) {
        return g -> new ShardedResultGroup(shard.getShard(), g.getKey(), g.getSeries(),
            g.getGroup(), g.getCadence());
    }

    public static MultiSummary summarize(List<ResultGroup> resultGroups) {
        final Histogram.Builder keySize = Histogram.builder();
        final SeriesSetsSummarizer seriesSummarizer = new SeriesSetsSummarizer();
        final Histogram.Builder dataSize = Histogram.builder();
        Optional<Long> cadence = Optional.empty();

        for (final ResultGroup rg : resultGroups) {
            keySize.add(rg.getKey().size());
            seriesSummarizer.add(rg.getSeries());
            dataSize.add(rg.getGroup().size());
            cadence = Optional.of(rg.getCadence());
        }

        return new MultiSummary(keySize.build(), seriesSummarizer.end(), dataSize.build(), cadence);
    }

    @Data
    public static class MultiSummary {
        private final Histogram keySize;
        private final SeriesSetsSummarizer.Summary series;
        private final Histogram dataSize;
        private final Optional<Long> cadence;
    }
}
