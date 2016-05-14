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

package com.spotify.heroic.statistics.semantic;

import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.FutureReporter.Context;
import com.spotify.heroic.statistics.LocalMetadataBackendReporter;
import com.spotify.heroic.statistics.LocalMetadataManagerReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.ToString;

@ToString(of = {})
public class SemanticMetadataManagerReporter implements LocalMetadataManagerReporter {
    private static final String COMPONENT = "metadata-backend-manager";

    private final SemanticMetricRegistry registry;

    private final FutureReporter refresh;
    private final FutureReporter findTags;
    private final FutureReporter findTimeSeries;
    private final FutureReporter countSeries;
    private final FutureReporter findKeys;
    private final FutureReporter tagKeySuggest;
    private final FutureReporter tagSuggest;
    private final FutureReporter keySuggest;
    private final FutureReporter tagValuesSuggest;
    private final FutureReporter tagValueSuggest;

    private final MetricId id;

    public SemanticMetadataManagerReporter(SemanticMetricRegistry registry) {
        this.id = MetricId.build().tagged("component", COMPONENT);

        this.registry = registry;

        refresh = new SemanticFutureReporter(registry,
            id.tagged("what", "refresh", "unit", Units.REFRESH));
        findTags = new SemanticFutureReporter(registry,
            id.tagged("what", "find-tags", "unit", Units.LOOKUP));
        findTimeSeries = new SemanticFutureReporter(registry,
            id.tagged("what", "find-time-series", "unit", Units.LOOKUP));
        countSeries = new SemanticFutureReporter(registry,
            id.tagged("what", "count-series", "unit", Units.LOOKUP));
        findKeys = new SemanticFutureReporter(registry,
            id.tagged("what", "find-keys", "unit", Units.LOOKUP));
        tagKeySuggest = new SemanticFutureReporter(registry,
            id.tagged("what", "tag-key-suggest", "unit", Units.LOOKUP));
        tagSuggest = new SemanticFutureReporter(registry,
            id.tagged("what", "tag-suggest", "unit", Units.LOOKUP));
        keySuggest = new SemanticFutureReporter(registry,
            id.tagged("what", "key-suggest", "unit", Units.LOOKUP));
        tagValuesSuggest = new SemanticFutureReporter(registry,
            id.tagged("what", "tag-values-suggest", "unit", Units.LOOKUP));
        tagValueSuggest = new SemanticFutureReporter(registry,
            id.tagged("what", "tag-value-suggest", "unit", Units.LOOKUP));
    }

    @Override
    public Context reportFindTags() {
        return findTags.setup();
    }

    @Override
    public Context reportFindTimeSeries() {
        return findTimeSeries.setup();
    }

    @Override
    public Context reportCountSeries() {
        return countSeries.setup();
    }

    @Override
    public Context reportFindKeys() {
        return findKeys.setup();
    }

    @Override
    public Context reportTagKeySuggest() {
        return tagKeySuggest.setup();
    }

    @Override
    public Context reportTagSuggest() {
        return tagSuggest.setup();
    }

    @Override
    public Context reportTagValuesSuggest() {
        return tagValuesSuggest.setup();
    }

    @Override
    public Context reportTagValueSuggest() {
        return tagValueSuggest.setup();
    }

    @Override
    public Context reportKeySuggest() {
        return keySuggest.setup();
    }

    @Override
    public LocalMetadataBackendReporter newMetadataBackend(String id) {
        return new SemanticLocalMetadataBackendReporter(registry, this.id.tagged("backend", id));
    }
}
