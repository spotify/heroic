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

import java.util.List;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicParameters;
import com.spotify.heroic.HeroicProfile;
import com.spotify.heroic.consumer.ConsumerModule;
import com.spotify.heroic.consumer.kafka.KafkaConsumerModule;

public class KafkaConsumerProfile extends HeroicProfileBase {
    private final Splitter splitter = Splitter.on(",").trimResults();

    @Override
    public HeroicConfig.Builder build(final HeroicParameters params) throws Exception {
        final ImmutableMap.Builder<String, String> config = ImmutableMap.builder();

        config.put("zookeeper.connect", params.require("kafka.zookeeper"));
        config.put("group.id", params.require("kafka.group"));

        final KafkaConsumerModule.Builder module = KafkaConsumerModule.builder().config(config.build());

        module.schema(params.require("kafka.schema"));

        params.get("kafka.topics").map(splitter::split).map(topics -> module.topics(ImmutableList.copyOf(topics)));

        // @formatter:off
        return HeroicConfig.builder()
            .consumers(ImmutableList.<ConsumerModule.Builder>builder().add(module).build());
        // @formatter:on
    }

    @Override
    public String description() {
        return "Configures a consumer for Kafka";
    }

    @Override
    public List<Option> options() {
        // @formatter:off
        return ImmutableList.of(
            HeroicProfile.option("kafka.zookeeper", "Connection string to Zookeeper", "<url>[,..][/prefix]"),
            HeroicProfile.option("kafka.group", "Consumer Group", "<group>"),
            HeroicProfile.option("kafka.topics", "Topics to consume from", "<topic>[,..]"),
            HeroicProfile.option("kafka.schema", "Schema Class to use", "<schema>")
        );
        // @formatter:on
    }
}
