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

package com.spotify.heroic.profile;

import static com.spotify.heroic.ParameterSpecification.parameter;

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.common.GrokProcessor;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.consumer.collectd.CollectdConsumerModule;

public class CollectdConsumerProfile extends HeroicProfileBase {
    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        final CollectdConsumerModule.Builder module = CollectdConsumerModule.builder();

        params.get("collectd.host").ifPresent(module::host);
        params.getInteger("collectd.port").ifPresent(module::port);
        params.get("collectd.pattern").map(p -> new GrokProcessor(ImmutableMap.of(), p))
                .ifPresent(module::hostProcessor);

        // @formatter:off
        return HeroicConfig.builder()
            .consumers(ImmutableList.<ConsumerModule.Builder>builder().add(module).build());
        // @formatter:on
    }

    @Override
    public String description() {
        return "Configures a consumer for Collectd";
    }

    @Override
    public List<ParameterSpecification> options() {
        // @formatter:off
        return ImmutableList.of(
            parameter("collectd.host", "Host to bind to", "<host>"),
            parameter("collectd.port", "Port to bind to", "<port>")
        );
        // @formatter:on
    }
}
