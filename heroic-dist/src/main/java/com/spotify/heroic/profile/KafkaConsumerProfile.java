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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.consumer.kafka.KafkaConsumerModule;

import java.util.List;
import java.util.Optional;

import static com.spotify.heroic.ParameterSpecification.parameter;

public class KafkaConsumerProfile extends HeroicProfileBase {
    private final Splitter splitter = Splitter.on(",").trimResults();

    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        final ImmutableMap.Builder<String, String> config = ImmutableMap.builder();

        config.put("zookeeper.connect", params.require("zookeeper"));
        config.put("group.id", params.require("group"));

        final KafkaConsumerModule.Builder module =
            KafkaConsumerModule.builder().config(config.build());

        module.schema(params.require("schema"));

        params
            .get("topics")
            .map(splitter::split)
            .map(topics -> module.topics(ImmutableList.copyOf(topics)));

        // @formatter:off
        return HeroicConfig.builder()
            .consumers(ImmutableList.<ConsumerModule.Builder>builder().add(module).build());
        // @formatter:on
    }

    @Override
    public Optional<String> scope() {
        return Optional.of("kafka");
    }

    @Override
    public String description() {
        return "Configures a consumer for Kafka";
    }

    @Override
    public List<ParameterSpecification> options() {
        // @formatter:off
        return ImmutableList.of(
            parameter("zookeeper", "Connection string to Zookeeper", "<url>[,..][/prefix]"),
            parameter("group", "Consumer Group", "<group>"),
            parameter("topics", "Topics to consume from", "<topic>[,..]"),
            parameter("schema", "Schema Class to use", "<schema>")
        );
        // @formatter:on
    }
}
