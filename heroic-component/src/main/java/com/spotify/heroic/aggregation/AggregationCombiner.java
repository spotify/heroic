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

package com.spotify.heroic.aggregation;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.metric.ShardedResultGroup;
import java.util.List;


public interface AggregationCombiner {

    List<ShardedResultGroup> combine(List<List<ShardedResultGroup>> all);

    AggregationCombiner DEFAULT = new AggregationCombiner() {
        @Override
        public List<ShardedResultGroup> combine(
            final List<List<ShardedResultGroup>> all
        ) {
            final ImmutableList.Builder<ShardedResultGroup> combined = ImmutableList.builder();

            for (final List<ShardedResultGroup> groups : all) {
                combined.addAll(groups);
            }

            return combined.build();
        }

        @Override
        public String toString() {
            return "DEFAULT";
        }
    };


    AggregationCombiner TDIGEST_DEFAULT = new AggregationCombiner() {
        @Override
        public List<ShardedResultGroup> combine(
            final List<List<ShardedResultGroup>> all
        ) {
            return TDigestAggregationCombiner.simpleCombine(all);
        }

        @Override
        public String toString() {
            return "TDIGEST_DEFAULT";
        }
    };
}
