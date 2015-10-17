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

package com.spotify.heroic.suggest.elasticsearch;

import static com.spotify.heroic.ParameterSpecification.parameter;

import java.util.List;

import javax.inject.Inject;

import com.google.common.collect.ImmutableList;
import com.spotify.heroic.HeroicConfigurationContext;
import com.spotify.heroic.HeroicModule;
import com.spotify.heroic.ParameterSpecification;

public class Module implements HeroicModule {
    @Override
    public Entry setup() {
        return new Entry() {
            @Inject
            private HeroicConfigurationContext context;

            @Override
            public void setup() {
                context.registerType("elasticsearch", ElasticsearchSuggestModule.class);
            }
        };
    }

    @Override
    public List<ParameterSpecification> parameters() {
        // @formatter:off
        return ImmutableList.of(
            parameter("elasticsearch.configure", "Automatically configure the Elasticsearch backend")
        );
        // @formatter:on
    }
}