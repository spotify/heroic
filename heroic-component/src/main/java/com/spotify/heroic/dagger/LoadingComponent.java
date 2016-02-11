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

package com.spotify.heroic.dagger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfiguration;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicLifeCycle;
import com.spotify.heroic.HeroicReporterConfiguration;
import com.spotify.heroic.aggregation.AggregationFactory;
import com.spotify.heroic.aggregation.AggregationRegistry;
import com.spotify.heroic.aggregation.AggregationSerializer;
import com.spotify.heroic.common.JavaxRestFramework;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.filter.FilterFactory;
import com.spotify.heroic.filter.FilterModifier;
import com.spotify.heroic.filter.FilterRegistry;
import com.spotify.heroic.filter.FilterSerializer;
import com.spotify.heroic.scheduler.Scheduler;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

import javax.inject.Named;
import java.util.concurrent.ExecutorService;

/**
 * The component responsible for the loading phase of heroic.
 * <p>
 * Depends this phase, Heroic loads all modules. Modules are components extending {@link
 * com.spotify.heroic.HeroicModule} that have been registered when configuring Heroic.
 */
public interface LoadingComponent {
    HeroicReporterConfiguration reporterConfig();

    ExtraParameters parameters();

    HeroicConfiguration options();

    @Named("common")
    SerializerFramework serializer();

    FilterRegistry filterRegistry();

    FilterSerializer filterSerializer();

    AggregationRegistry aggregationRegistry();

    AggregationFactory aggregationFactory();

    AggregationSerializer aggregationSerializer();

    Serializer<Series> series();

    AsyncFramework async();

    @Named("application/heroic-config")
    ObjectMapper configMapper();

    Scheduler scheduler();

    HeroicLifeCycle lifeCycle();

    FilterFactory filterFactory();

    FilterModifier filterModifier();

    ExecutorService executorService();

    JavaxRestFramework javaxRestFramework();

    HeroicConfigurationContext heroicConfigurationContext();
}
