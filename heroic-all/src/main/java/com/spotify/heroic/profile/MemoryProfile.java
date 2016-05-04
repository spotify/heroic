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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.ExtraParameters;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.ParameterSpecification;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.elasticsearch.ConnectionModule;
import com.spotify.heroic.elasticsearch.StandaloneClientSetup;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metadata.elasticsearch.ElasticsearchMetadataModule;
import com.spotify.heroic.metadata.memory.MemoryMetadataModule;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.memory.MemoryMetricModule;
import com.spotify.heroic.suggest.SuggestManagerModule;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.suggest.elasticsearch.ElasticsearchSuggestModule;

import java.util.List;
import java.util.Optional;

import static com.spotify.heroic.ParameterSpecification.parameter;

public class MemoryProfile extends HeroicProfileBase {
    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        final HeroicConfig.Builder builder = HeroicConfig.builder();

        final boolean elasticsearch = params.getBoolean("elasticsearch").orElse(false);
        final boolean synchronizedStorage = params.getBoolean("synchronizedStorage").orElse(false);

        if (!elasticsearch) {
            builder.metadata(MetadataManagerModule
                .builder()
                .backends(ImmutableList.<MetadataModule>of(MemoryMetadataModule
                    .builder()
                    .synchronizedStorage(synchronizedStorage)
                    .build())));
        }

        if (elasticsearch) {
            builder.metadata(MetadataManagerModule
                .builder()
                .backends(ImmutableList.<MetadataModule>of(ElasticsearchMetadataModule
                    .builder()
                    .connection(ConnectionModule
                        .builder()
                        .clientSetup(StandaloneClientSetup.builder().build())
                        .index(
                            RotatingIndexMapping.builder().pattern("heroic-metadata-v1-%s").build())
                        .build())
                    .build())));
        }

        if (elasticsearch) {
            builder.suggest(SuggestManagerModule
                .builder()
                .backends(ImmutableList.<SuggestModule>of(ElasticsearchSuggestModule
                    .builder()
                    .connection(ConnectionModule
                        .builder()
                        .clientSetup(StandaloneClientSetup.builder().build())
                        .index(
                            RotatingIndexMapping.builder().pattern("heroic-suggest-v1-%s").build())
                        .build())
                    .build())));
        }

        // @formatter:off
        return builder
            .cluster(
                ClusterManagerModule.builder()
                .tags(ImmutableMap.of("site", "local"))
            )
            .metric(
                MetricManagerModule.builder()
                    .backends(ImmutableList.<MetricModule>of(
                        MemoryMetricModule.builder()
                            .synchronizedStorage(synchronizedStorage)
                            .build()
                    ))
            );
        // @formatter:on
    }

    @Override
    public Optional<String> scope() {
        return Optional.of("memory");
    }

    @Override
    public String description() {
        // @formatter:off
        return "Configures in-memory backends for everything (useful for integration/performance " +
                "testing)";
        // @formatter:on
    }

    @Override
    public List<ParameterSpecification> options() {
        // @formatter:off
        return ImmutableList.of(
            parameter("elasticsearch", "If set, use real elasticsearch backends"),
            parameter("synchronized", "If set, synchronized storage for happens-before " +
                    "behavior")
        );
        // @formatter:on
    }
}
