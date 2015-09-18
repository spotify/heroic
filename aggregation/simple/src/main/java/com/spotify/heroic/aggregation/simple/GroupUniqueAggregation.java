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

package com.spotify.heroic.aggregation.simple;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.BucketAggregation;
import com.spotify.heroic.metric.Metric;
import com.spotify.heroic.metric.MetricGroup;
import com.spotify.heroic.metric.MetricType;
import com.spotify.heroic.metric.MetricCollection;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true, of = { "NAME" })
public class GroupUniqueAggregation extends BucketAggregation<GroupUniqueBucket> {
    public static final String NAME = "group-unique";

    @JsonCreator
    public GroupUniqueAggregation(@JsonProperty("size") final long size, @JsonProperty("extent") final long extent) {
        super(size, extent, BucketAggregation.ALL_TYPES, MetricType.GROUP);
    }

    @Override
    protected GroupUniqueBucket buildBucket(long timestamp) {
        return new GroupUniqueBucket(timestamp);
    }

    @Override
    protected Metric build(final GroupUniqueBucket bucket) {
        final List<MetricCollection> groups = bucket.groups();

        if (groups.isEmpty()) {
            return Metric.invalid();
        }

        return new MetricGroup(bucket.timestamp(), groups);
    }
}