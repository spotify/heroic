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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationState;
import com.spotify.heroic.aggregation.AggregationTraversal;
import com.spotify.heroic.aggregation.ReducerSession;
import com.spotify.heroic.common.DateRange;

import java.util.List;

public class AboveKInstance implements FilterKInstance {
    private final FilterKThresholdStrategy strategy;
    private final FilterAggregation aggregation;

    @JsonCreator
    public AboveKInstance(@JsonProperty("k") double k, @JsonProperty("of") AggregationInstance of) {
        strategy = new FilterKThresholdStrategy(FilterKThresholdType.ABOVE, k);
        aggregation = new FilterAggregation(strategy, of);
    }

    @Override
    public long estimate(DateRange range) {
        return aggregation.estimate(range);
    }

    @Override
    public long cadence() {
        return aggregation.cadence();
    }

    @Override
    public AggregationTraversal session(List<AggregationState> states, DateRange range) {
        return aggregation.session(states, range);
    }

    @Override
    public ReducerSession reducer(DateRange range) {
        return aggregation.reducer(range);
    }

    public double getK() {
        return strategy.getK();
    }

    @Override
    public AggregationInstance getOf() {
        return aggregation.getOf();
    }
}
