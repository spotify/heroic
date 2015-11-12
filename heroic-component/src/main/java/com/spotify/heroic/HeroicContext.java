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

import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationDSL;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.MultiArgumentsFilter;
import com.spotify.heroic.filter.NoArgumentFilter;
import com.spotify.heroic.filter.OneArgumentFilter;
import com.spotify.heroic.filter.TwoArgumentsFilter;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.serializer.Serializer;

public interface HeroicContext {
    <T extends AggregationInstance, R extends Aggregation> void aggregation(String id,
            Class<T> type, Class<R> queryType, Serializer<T> serializer, AggregationDSL builder);

    <T extends Filter.OneArg<A>, I extends T, A> void filter(String typeId, Class<T> type,
            Class<I> impl, OneArgumentFilter<T, A> builder, Serializer<A> first);

    <T extends Filter.TwoArgs<A, B>, I extends T, A, B> void filter(String typeId, Class<T> type,
            Class<I> impl, TwoArgumentsFilter<T, A, B> builder, Serializer<A> first,
            Serializer<B> second);

    <T extends Filter.MultiArgs<A>, I extends T, A> void filter(String typeId, Class<T> type,
            Class<I> impl, MultiArgumentsFilter<T, A> builder, Serializer<A> term);

    <T extends Filter.NoArg, I extends T> void filter(String typeId, Class<T> type, Class<I> impl,
            NoArgumentFilter<T> builder);

    /**
     * Future that will be resolved after all services have been started.
     */
    AsyncFuture<Void> startedFuture();
}
