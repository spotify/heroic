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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.cluster.ClusterNode;
import lombok.Data;

import java.util.Map;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkNotNull;

@Data
public class ResultGroup {
    final Map<String, String> key;
    final SeriesValues series;
    final MetricCollection group;
    /**
     * The interval in milliseconds for which a sample can be expected. A cadence of 0 indicates
     * that this value is unknown.
     */
    final long cadence;

    @JsonCreator
    public ResultGroup(
        @JsonProperty("key") Map<String, String> key, @JsonProperty("series") SeriesValues series,
        @JsonProperty("group") MetricCollection group, @JsonProperty("cadence") Long cadence
    ) {
        this.key = checkNotNull(key, "key");
        this.series = checkNotNull(series, "series");
        this.group = checkNotNull(group, "group");
        this.cadence = checkNotNull(cadence, "cadence");
    }

    public static Function<? super ResultGroup, ? extends ShardedResultGroup> toShardedResultGroup(
        final ClusterNode c
    ) {
        return (g) -> new ShardedResultGroup(c.metadata().getTags(), g.getKey(), g.getSeries(),
            g.getGroup(), g.getCadence());
    }
}
