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

package com.spotify.heroic.metadata.elasticsearch;

import com.spotify.heroic.elasticsearch.ConnectionModule;
import com.spotify.heroic.elasticsearch.TransportClientSetup;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.test.AbstractMetadataBackendIT;
import com.spotify.heroic.test.ElasticSearchTestContainer;
import java.util.List;
import java.util.UUID;

public class MetadataBackendKVIT extends AbstractMetadataBackendIT {
    private final static ElasticSearchTestContainer esContainer;

    static {
        esContainer = ElasticSearchTestContainer.getInstance();
    }

    private String backendType() {
        return "kv";
    }

    @Override
    protected void setupConditions() {
        super.setupConditions();

        // TODO: support findTags?
        findTagsSupport = false;
    }

    @Override
    protected MetadataModule setupModule() throws Exception {
        final String testName = "heroic-it-" + UUID.randomUUID().toString();

        final RotatingIndexMapping index =
            RotatingIndexMapping.builder().pattern(testName + "-%s").build();

        return ElasticsearchMetadataModule
            .builder()
            .templateName(testName)
            .configure(true)
            .backendType(backendType())
            .connection(ConnectionModule
                .builder()
                .index(index)
                .clientSetup(TransportClientSetup.builder()
                    .clusterName("docker-cluster")
                    .seeds(List.of(
                        esContainer.getTcpHost().getHostName()
                        + ":" + esContainer.getTcpHost().getPort()))
                    .build())
                .build())
            .build();
    }
}
