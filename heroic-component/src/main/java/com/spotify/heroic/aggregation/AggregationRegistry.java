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

import com.fasterxml.jackson.databind.Module;
import eu.toolchain.serializer.Serializer;

public interface AggregationRegistry {
    /**
     * Register a new aggregation.
     *
     * @param id The id of the new aggregation, will be used in the type field, and in the DSL.
     * @param type The type of the aggregation.
     * @param instanceType The type of the instance.
     * @param instanceSerializer Serializer for the aggregation instance.
     * @param dsl DSL factory for the aggregation.
     */
    <A extends Aggregation, I extends AggregationInstance> void register(
        String id, Class<A> type, Class<I> instanceType, Serializer<I> instanceSerializer,
        AggregationDSL dsl
    );

    Module module();

    AggregationFactory newAggregationFactory();

    AggregationSerializer newAggregationSerializer();
}
