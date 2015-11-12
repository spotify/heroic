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

import javax.inject.Inject;

import com.spotify.heroic.aggregation.Aggregation;
import com.spotify.heroic.aggregation.AggregationDSL;
import com.spotify.heroic.aggregation.AggregationInstance;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.filter.Filter;
import com.spotify.heroic.filter.Filter.MultiArgs;
import com.spotify.heroic.filter.Filter.NoArg;
import com.spotify.heroic.filter.Filter.OneArg;
import com.spotify.heroic.filter.Filter.TwoArgs;
import com.spotify.heroic.filter.FilterJsonDeserializer;
import com.spotify.heroic.filter.FilterJsonSerialization;
import com.spotify.heroic.filter.FilterJsonSerializer;
import com.spotify.heroic.filter.FilterSerializer;
import com.spotify.heroic.filter.MultiArgumentsFilter;
import com.spotify.heroic.filter.NoArgumentFilter;
import com.spotify.heroic.filter.OneArgumentFilter;
import com.spotify.heroic.filter.TwoArgumentsFilter;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.serializer.Serializer;

public class CoreHeroicContext implements HeroicContext {
    @Inject
    private AggregationSerializer aggregationSerializer;

    @Inject
    private FilterSerializer filterSerializer;

    @Inject
    private FilterJsonSerializer filterJsonSerializer;

    @Inject
    private FilterJsonDeserializer filterJsonDeserializer;

    @Inject
    private AsyncFramework async;

    private final Object lock = new Object();
    private volatile ResolvableFuture<Void> startedFuture;

    @Override
    public <T extends AggregationInstance, R extends Aggregation> void aggregation(String id,
            Class<T> type, Class<R> queryType, Serializer<T> serializer, AggregationDSL builder) {
        aggregationSerializer.register(id, type, serializer, builder);
        aggregationSerializer.registerQuery(id, queryType);
    }

    @Override
    public <T extends OneArg<A>, I extends T, A> void filter(String id, Class<T> type,
            Class<I> impl, OneArgumentFilter<T, A> builder, Serializer<A> first) {
        filterJson(id, type, impl, builder);
        filterSerializer.register(id, type, builder, first);
    }

    @Override
    public <T extends TwoArgs<A, B>, I extends T, A, B> void filter(String id, Class<T> type,
            Class<I> impl, TwoArgumentsFilter<T, A, B> builder, Serializer<A> first,
            Serializer<B> second) {
        filterJson(id, type, impl, builder);
        filterSerializer.register(id, type, builder, first, second);
    }

    @Override
    public <T extends MultiArgs<A>, I extends T, A> void filter(String id, Class<T> type,
            Class<I> impl, MultiArgumentsFilter<T, A> builder, Serializer<A> term) {
        filterJson(id, type, impl, builder);
        filterSerializer.register(id, type, builder, term);
    }

    @Override
    public <T extends NoArg, I extends T> void filter(String id, Class<T> type, Class<I> impl,
            NoArgumentFilter<T> builder) {
        filterJson(id, type, impl, builder);
        filterSerializer.register(id, type, builder);
    }

    public <T extends Filter, I extends T> void filterJson(String id, Class<T> type, Class<I> impl,
            FilterJsonSerialization<T> serialization) {
        filterJsonSerializer.register(impl, serialization);
        filterJsonDeserializer.register(id, serialization);
    }

    @Override
    public AsyncFuture<Void> startedFuture() {
        synchronized (lock) {
            if (this.startedFuture == null) {
                this.startedFuture = async.future();
            }
        }

        return this.startedFuture;
    }

    public void resolveCoreFuture() {
        if (this.startedFuture != null) {
            this.startedFuture.resolve(null);
        }
    }
}
