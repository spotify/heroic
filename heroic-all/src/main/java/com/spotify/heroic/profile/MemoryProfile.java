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
import com.spotify.heroic.aggregationcache.AggregationCacheModule;
import com.spotify.heroic.aggregationcache.InMemoryAggregationCacheBackendConfig;
import com.spotify.heroic.cluster.ClusterManagerModule;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.elasticsearch.StandaloneClientSetup;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metadata.elasticsearch.ElasticsearchMetadataModule;
import com.spotify.heroic.metric.MetricManagerModule;
import com.spotify.heroic.metric.MetricModule;
import com.spotify.heroic.metric.memory.MemoryMetricModule;
import com.spotify.heroic.suggest.SuggestManagerModule;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.suggest.elasticsearch.ElasticsearchSuggestModule;

public class MemoryProfile extends HeroicProfileBase {
    @Override
    public HeroicConfig.Builder build(final ExtraParameters params) throws Exception {
        // @formatter:off
        return HeroicConfig.builder()
            .cluster(
                ClusterManagerModule.builder()
                .tags(ImmutableMap.of("site", "local"))
            )
            .metric(
                MetricManagerModule.builder()
                    .backends(ImmutableList.<MetricModule>of(
                        MemoryMetricModule.builder()
                            .build()
                    ))
            )
            .metadata(
                MetadataManagerModule.builder()
                    .backends(ImmutableList.<MetadataModule>of(
                        ElasticsearchMetadataModule.builder()
                            .connection(
                                ManagedConnectionFactory.builder()
                                .clientSetup(
                                    StandaloneClientSetup.builder().build()
                                )
                                .index(
                                    RotatingIndexMapping.builder()
                                    .pattern("heroic-metadata-v1-%s")
                                    .build()
                                )
                                .build()
                            )
                            .build()
                    ))
            )
            .suggest(
                SuggestManagerModule.builder()
                    .backends(ImmutableList.<SuggestModule>of(
                        ElasticsearchSuggestModule.builder()
                            .connection(
                                ManagedConnectionFactory.builder()
                                .clientSetup(
                                    StandaloneClientSetup.builder().build()
                                )
                                .index(
                                    RotatingIndexMapping.builder()
                                    .pattern("heroic-suggest-v1-%s")
                                    .build()
                                )
                                .build()
                            )
                            .build()
                    ))
            )
            .cache(
                AggregationCacheModule.builder()
                    .backend(InMemoryAggregationCacheBackendConfig.builder().build())
            );
        // @formatter:on
    }

    @Override
    public String description() {
        // @formatter:off
        return "Configures in-memory backends for everything (useful for integration/performance testing)";
        // @formatter:on
    }
}
