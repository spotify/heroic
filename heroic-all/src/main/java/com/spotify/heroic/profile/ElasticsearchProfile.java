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
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicParameters;
import com.spotify.heroic.HeroicProfile;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.elasticsearch.index.IndexMapping;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metadata.elasticsearch.ElasticsearchMetadataModule;
import com.spotify.heroic.shell.ShellServerModule;

public class ElasticsearchProfile implements HeroicProfile {
    private static final Splitter splitter = Splitter.on(',').trimResults();

    @Override
    public HeroicConfig.Builder build(final HeroicParameters params) throws Exception {
        final String backendType = params.get("elasticsearch.type").orNull();
        final String clusterName = params.get("elasticsearch.clusterName").orNull();
        final String pattern = params.get("elasticsearch.pattern").orNull();

        final List<String> seeds = params.get("elasticsearch.seeds")
                .transform(s -> ImmutableList.copyOf(splitter.split(s))).or(ImmutableList.of("localhost"));

        final IndexMapping index = RotatingIndexMapping.builder().pattern(pattern).build();

        final ManagedConnectionFactory connection = ManagedConnectionFactory.builder().clusterName(clusterName)
                .seeds(seeds).index(index).build();

        // @formatter:off
        return HeroicConfig.builder()
            .metadata(
                MetadataManagerModule.builder()
                    .backends(ImmutableList.<MetadataModule>of(
                        ElasticsearchMetadataModule.builder()
                        .connection(connection)
                        .backendType(backendType)
                        .build()
                    ))
            )
            .shellServer(ShellServerModule.builder());
        // @formatter:on
    }

    @Override
    public String description() {
        return "Configure Elasticsearch Metadata";
    }
}
