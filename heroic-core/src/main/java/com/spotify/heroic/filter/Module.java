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

import com.google.common.collect.Lists;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.dagger.LoadingComponent;
import dagger.Component;
import eu.toolchain.serializer.SerializerFramework;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;

public class Module implements HeroicModule {
    @Override
    public Entry setup(LoadingComponent loading) {
        return DaggerModule_C.builder().loadingComponent(loading).build().entry();
    }

    @Component(dependencies = LoadingComponent.class)
    interface C {
        E entry();
    }

    static class E implements HeroicModule.Entry {
        private final FilterRegistry c;
        private final SerializerFramework s;
        private final FilterSerializer filter;

        @Inject
        public E(
            FilterRegistry c, @Named("common") SerializerFramework s, FilterSerializer filter
        ) {
            this.c = c;
            this.s = s;
            this.filter = filter;
        }

        @Override
        public void setup() {
            c.register(AndFilter.OPERATOR, AndFilter.class,
                new MultiArgumentsFilterBase<AndFilter, Filter>(SerializerCommon.FILTER) {
                    @Override
                    public AndFilter build(Collection<Filter> filters) {
                        return new AndFilter(Lists.newArrayList(filters));
                    }
                }, filter);

            c.register(OrFilter.OPERATOR, OrFilter.class,
                new MultiArgumentsFilterBase<OrFilter, Filter>(SerializerCommon.FILTER) {
                    @Override
                    public OrFilter build(Collection<Filter> filters) {
                        return new OrFilter(Lists.newArrayList(filters));
                    }
                }, filter);

            c.register(NotFilter.OPERATOR, NotFilter.class,
                new OneArgumentFilterBase<NotFilter, Filter>(SerializerCommon.FILTER) {
                    @Override
                    public NotFilter build(Filter filter) {
                        return new NotFilter(filter);
                    }
                }, filter);

            c.register(MatchKeyFilter.OPERATOR, MatchKeyFilter.class,
                new OneArgumentFilterBase<MatchKeyFilter, String>(SerializerCommon.STRING) {
                    @Override
                    public MatchKeyFilter build(String first) {
                        return new MatchKeyFilter(first);
                    }
                }, s.string());

            c.register(MatchTagFilter.OPERATOR, MatchTagFilter.class,
                new TwoArgumentsFilterBase<MatchTagFilter, String, String>(SerializerCommon.STRING,
                    SerializerCommon.STRING) {
                    @Override
                    public MatchTagFilter build(String first, String second) {
                        return new MatchTagFilter(first, second);
                    }
                }, s.string(), s.string());

            c.register(HasTagFilter.OPERATOR, HasTagFilter.class,
                new OneArgumentFilterBase<HasTagFilter, String>(SerializerCommon.STRING) {
                    @Override
                    public HasTagFilter build(String first) {
                        return new HasTagFilter(first);
                    }
                }, s.string());

            c.register(StartsWithFilter.OPERATOR, StartsWithFilter.class,
                new TwoArgumentsFilterBase<StartsWithFilter, String, String>(
                    SerializerCommon.STRING, SerializerCommon.STRING) {
                    @Override
                    public StartsWithFilter build(String first, String second) {
                        return new StartsWithFilter(first, second);
                    }
                }, s.string(), s.string());

            c.register(RegexFilter.OPERATOR, RegexFilter.class,
                new TwoArgumentsFilterBase<RegexFilter, String, String>(SerializerCommon.STRING,
                    SerializerCommon.STRING) {
                    @Override
                    public RegexFilter build(String first, String second) {
                        return new RegexFilter(first, second);
                    }
                }, s.string(), s.string());

            c.register(TrueFilter.OPERATOR, TrueFilter.class,
                new NoArgumentFilterBase<TrueFilter>() {
                    @Override
                    public TrueFilter build() {
                        return TrueFilter.get();
                    }
                });

            c.register(FalseFilter.OPERATOR, FalseFilter.class,
                new NoArgumentFilterBase<FalseFilter>() {
                    @Override
                    public FalseFilter build() {
                        return FalseFilter.get();
                    }
                });

            c.register(RawFilter.OPERATOR, RawFilter.class,
                new OneArgumentFilterBase<RawFilter, String>(SerializerCommon.STRING) {
                    @Override
                    public RawFilter build(String first) {
                        return new RawFilter(first);
                    }
                }, s.string());
        }
    }
}
