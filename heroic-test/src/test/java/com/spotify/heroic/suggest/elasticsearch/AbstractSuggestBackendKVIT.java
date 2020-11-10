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

package com.spotify.heroic.suggest.elasticsearch;

import static org.junit.Assert.assertEquals;

import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.elasticsearch.ClientWrapper;
import com.spotify.heroic.elasticsearch.ConnectionModule;
import com.spotify.heroic.elasticsearch.index.RotatingIndexMapping;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.suggest.SuggestModule;
import com.spotify.heroic.suggest.WriteSuggest;
import com.spotify.heroic.test.AbstractSuggestBackendIT;
import com.spotify.heroic.test.ElasticSearchTestContainer;
import com.spotify.heroic.test.TimestampPrepender.EntityType;
import org.junit.Test;

public abstract class AbstractSuggestBackendKVIT extends AbstractSuggestBackendIT {
    final static ElasticSearchTestContainer esContainer;

    static {
        esContainer = ElasticSearchTestContainer.getInstance();
    }

    protected abstract ClientWrapper setupClient();

    @Override
    protected SuggestModule setupModule() {
        RotatingIndexMapping index =
            RotatingIndexMapping.builder().pattern(testName + "-%s").build();

        return ElasticsearchSuggestModule
            .builder()
            .templateName(testName)
            .configure(true)
            .backendType("kv")
            .writeCacheMaxSize(0)
            .connection(ConnectionModule
                .builder()
                .index(index)
                .clientSetup(setupClient())
                .build())
            .build();
    }

    @Test
    public void writeDuplicatesReturnErrorInResponse() throws Exception {
        var smallTestSeries =
            createSmallSeries(0L, EntityType.KEY);

        final WriteSuggest firstWrite =
            backend.write(new WriteSuggest.Request(smallTestSeries.get(0).getKey(),
                UNIVERSAL_RANGE)).get();
        final WriteSuggest secondWrite =
            backend.write(new WriteSuggest.Request(smallTestSeries.get(0).getKey(),
                UNIVERSAL_RANGE)).get();

        assertEquals(0, firstWrite.getErrors().size());
        assertEquals(2, secondWrite.getErrors().size());

        for (final RequestError e : secondWrite.getErrors()) {
            assert (e.toString().contains("VersionConflictEngineException") ||
                    e.toString().contains("version_conflict_engine_exception"));
        }
    }
}
