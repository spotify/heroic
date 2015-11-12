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

package com.spotify.heroic.filter;

import java.util.Collection;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.spotify.heroic.HeroicContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.filter.impl.AndFilterImpl;
import com.spotify.heroic.filter.impl.FalseFilterImpl;
import com.spotify.heroic.filter.impl.HasTagFilterImpl;
import com.spotify.heroic.filter.impl.MatchKeyFilterImpl;
import com.spotify.heroic.filter.impl.MatchTagFilterImpl;
import com.spotify.heroic.filter.impl.NotFilterImpl;
import com.spotify.heroic.filter.impl.OrFilterImpl;
import com.spotify.heroic.filter.impl.RawFilterImpl;
import com.spotify.heroic.filter.impl.RegexFilterImpl;
import com.spotify.heroic.filter.impl.SerializerCommon;
import com.spotify.heroic.filter.impl.StartsWithFilterImpl;
import com.spotify.heroic.filter.impl.TrueFilterImpl;

import eu.toolchain.serializer.SerializerFramework;

public class Module implements HeroicModule {
    @Override
    public Entry setup() {
        return new Entry() {
            @Inject
            private HeroicContext ctx;

            @Inject
            @Named("common")
            private SerializerFramework s;

            @Inject
            private FilterSerializer filter;

            @Override
            public void setup() {
                ctx.filter(AndFilterImpl.OPERATOR, Filter.And.class, AndFilterImpl.class,
                        new MultiArgumentsFilterBase<Filter.And, Filter>(SerializerCommon.FILTER) {
                    @Override
                    public Filter.And build(Collection<Filter> filters) {
                        return new AndFilterImpl(Lists.newArrayList(filters));
                    }
                }, filter);

                ctx.filter(OrFilterImpl.OPERATOR, Filter.Or.class, OrFilterImpl.class,
                        new MultiArgumentsFilterBase<Filter.Or, Filter>(SerializerCommon.FILTER) {
                    @Override
                    public Filter.Or build(Collection<Filter> filters) {
                        return new OrFilterImpl(Lists.newArrayList(filters));
                    }
                }, filter);

                ctx.filter(NotFilterImpl.OPERATOR, Filter.Not.class, NotFilterImpl.class,
                        new OneArgumentFilterBase<Filter.Not, Filter>(SerializerCommon.FILTER) {
                    @Override
                    public Filter.Not build(Filter filter) {
                        return new NotFilterImpl(filter);
                    }
                }, filter);

                ctx.filter(MatchKeyFilterImpl.OPERATOR, Filter.MatchKey.class,
                        MatchKeyFilterImpl.class,
                        new OneArgumentFilterBase<Filter.MatchKey, String>(
                                SerializerCommon.STRING) {
                    @Override
                    public Filter.MatchKey build(String first) {
                        return new MatchKeyFilterImpl(first);
                    }
                }, s.string());

                ctx.filter(MatchTagFilterImpl.OPERATOR, Filter.MatchTag.class,
                        MatchTagFilterImpl.class,
                        new TwoArgumentsFilterBase<Filter.MatchTag, String, String>(
                                SerializerCommon.STRING, SerializerCommon.STRING) {
                    @Override
                    public Filter.MatchTag build(String first, String second) {
                        return new MatchTagFilterImpl(first, second);
                    }
                }, s.string(), s.string());

                ctx.filter(HasTagFilterImpl.OPERATOR, Filter.HasTag.class, HasTagFilterImpl.class,
                        new OneArgumentFilterBase<Filter.HasTag, String>(SerializerCommon.STRING) {
                    @Override
                    public Filter.HasTag build(String first) {
                        return new HasTagFilterImpl(first);
                    }
                }, s.string());

                ctx.filter(StartsWithFilterImpl.OPERATOR, Filter.StartsWith.class,
                        StartsWithFilterImpl.class,
                        new TwoArgumentsFilterBase<Filter.StartsWith, String, String>(
                                SerializerCommon.STRING, SerializerCommon.STRING) {
                    @Override
                    public Filter.StartsWith build(String first, String second) {
                        return new StartsWithFilterImpl(first, second);
                    }
                }, s.string(), s.string());

                ctx.filter(RegexFilterImpl.OPERATOR, Filter.Regex.class, RegexFilterImpl.class,
                        new TwoArgumentsFilterBase<Filter.Regex, String, String>(
                                SerializerCommon.STRING, SerializerCommon.STRING) {
                    @Override
                    public Filter.Regex build(String first, String second) {
                        return new RegexFilterImpl(first, second);
                    }
                }, s.string(), s.string());

                ctx.filter(TrueFilterImpl.OPERATOR, Filter.True.class, TrueFilterImpl.class,
                        new NoArgumentFilterBase<Filter.True>() {
                    @Override
                    public Filter.True build() {
                        return TrueFilterImpl.get();
                    }
                });

                ctx.filter(FalseFilterImpl.OPERATOR, Filter.False.class, FalseFilterImpl.class,
                        new NoArgumentFilterBase<Filter.False>() {
                    @Override
                    public Filter.False build() {
                        return FalseFilterImpl.get();
                    }
                });

                ctx.filter(RawFilterImpl.OPERATOR, Filter.Raw.class, RawFilterImpl.class,
                        new OneArgumentFilterBase<Filter.Raw, String>(SerializerCommon.STRING) {
                    @Override
                    public Filter.Raw build(String first) {
                        return new RawFilterImpl(first);
                    }
                }, s.string());
            }
        };
    }
}
