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

import org.elasticsearch.common.collect.ImmutableList;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.spotify.heroic.HeroicConfig;
import com.spotify.heroic.HeroicParameters;
import com.spotify.heroic.HeroicProfile;
import com.spotify.heroic.elasticsearch.ManagedConnectionFactory;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.metadata.MetadataManagerModule;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.metadata.elasticsearch.ElasticsearchMetadataModule;

public class ElasticsearchMetadataProfile extends HeroicProfileBase {
    private static final Splitter splitter = Splitter.on(',').trimResults();

    @Override
    public HeroicConfig.Builder build(final HeroicParameters params) throws Exception {
        final RotatingIndexMapping.Builder index = RotatingIndexMapping.builder();

        params.get("elasticsearch.pattern").map(index::pattern);

        final ManagedConnectionFactory.Builder connection = ManagedConnectionFactory.builder().index(index.build());

        params.get("elasticsearch.clusterName").map(connection::clusterName);
        params.get("elasticsearch.seeds").map(s -> connection.seeds(ImmutableList.copyOf(splitter.split(s))));

        final ElasticsearchMetadataModule.Builder module = ElasticsearchMetadataModule.builder()
                .connection(connection.build());

        params.get("elasticsearch.type").map(module::backendType);

        return HeroicConfig.builder()
                .metadata(MetadataManagerModule.builder().backends(ImmutableList.<MetadataModule> of(module.build())));
    }

    @Override
    public String description() {
        return "Configures a metadata backend for Elasticsearch";
    }

    static final Joiner arguments = Joiner.on(", ");

    @Override
    public List<Option> options() {
        // @formatter:off
        return ImmutableList.of(
            HeroicProfile.option("elasticsearch.pattern", "Index pattern to use (example: heroic-%s)", "<pattern>"),
            HeroicProfile.option("elasticsearch.clusterName", "Cluster name to connect to", "<string>"),
            HeroicProfile.option("elasticsearch.seeds", "Seeds to connect to", "<host>[:<port][,..]"),
            HeroicProfile.option("elasticsearch.type", "Backend type to use, available types are: " + arguments.join(ElasticsearchMetadataModule.types()), "<type>")
        );
        // @formatter:on
    }
}
