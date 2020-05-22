/*
 * Copyright (c) 2020 Spotify AB.
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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableSet;
import com.spotify.heroic.common.Feature;
import com.spotify.heroic.common.FeatureSet;
import com.spotify.heroic.common.Features;
import com.spotify.heroic.common.OptionalLimit;
import com.spotify.heroic.elasticsearch.ClientWrapper;
import com.spotify.heroic.elasticsearch.ConnectionModule;
import com.spotify.heroic.elasticsearch.SearchTransformResult;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.filter.TrueFilter;
import com.spotify.heroic.metadata.FindSeries;
import com.spotify.heroic.metadata.MetadataModule;
import com.spotify.heroic.test.AbstractMetadataBackendIT;
import com.spotify.heroic.test.ElasticSearchTestContainer;
import java.util.Set;
import org.junit.Test;

public abstract class AbstractMetadataBackendKVIT extends AbstractMetadataBackendIT {
    final static ElasticSearchTestContainer esContainer;

    static {
        esContainer = ElasticSearchTestContainer.getInstance();
    }

    protected abstract ClientWrapper setupClient();

    @Override
    protected void setupConditions() {
        // TODO: support findTags?
        findTagsSupport = false;

        additionalFeatures = FeatureSet.of(Feature.METADATA_LIVE_CURSOR);
    }

    @Override
    protected MetadataModule setupModule() {
        RotatingIndexMapping index =
            RotatingIndexMapping.builder().pattern(testName + "-%s").build();

        return ElasticsearchMetadataModule
            .builder()
            .templateName(testName)
            .configure(true)
            .backendType("kv")
            .connection(ConnectionModule
                .builder()
                .index(index)
                .clientSetup(setupClient())
                .build())
            .scrollSize(numSeries / 2)
            .build();
    }

    @Test
    public void testHashField() throws Exception {
        FindSeries.Request f = new FindSeries.Request(
            TrueFilter.get(), range, OptionalLimit.empty(), Features.DEFAULT);

        Set<String> hashes = ((MetadataBackendKV) backend).entries(
            f,
            hit -> (String) hit.getSourceAsMap().get("hash"),
            SearchTransformResult::getSet,
            request -> { }
        ).get();

        assertEquals(ImmutableSet.of(s1.hash(), s2.hash(), s3.hash()), hashes);
    }
}
