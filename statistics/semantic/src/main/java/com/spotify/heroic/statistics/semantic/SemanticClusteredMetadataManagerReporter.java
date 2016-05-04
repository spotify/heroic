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

import com.spotify.heroic.statistics.ClusteredMetadataManagerReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.heroic.statistics.FutureReporter.Context;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ToString(of = {})
public class SemanticClusteredMetadataManagerReporter implements ClusteredMetadataManagerReporter {
    private static final String COMPONENT = "clustered-metadata-manager";

    private final SemanticMetricRegistry registry;

    private final FutureReporter findTags;
    private final MetricId findTagsShard;

    private final FutureReporter findKeys;
    private final MetricId findKeysShard;

    private final FutureReporter findSeries;
    private final MetricId findSeriesShard;

    private volatile Map<Map<String, String>, Shard> shards = new HashMap<>();

    private final FutureReporter deleteSeries;
    private final FutureReporter write;
    private final FutureReporter search;
    private final FutureReporter tagKeySuggest;
    private final FutureReporter tagSuggest;
    private final FutureReporter count;
    private final FutureReporter keySuggest;
    private final FutureReporter tagValuesSuggest;
    private final FutureReporter tagValueSuggest;

    public SemanticClusteredMetadataManagerReporter(SemanticMetricRegistry registry) {
        this.registry = registry;

        final MetricId id = MetricId.build().tagged("component", COMPONENT);

        this.findTags = new SemanticFutureReporter(registry,
            id.tagged("what", "find-tags", "unit", Units.READ));
        this.findTagsShard = id.tagged("what", "find-tags-shard", "unit", Units.READ);

        this.findKeys = new SemanticFutureReporter(registry,
            id.tagged("what", "find-keys", "unit", Units.READ));
        this.findKeysShard = id.tagged("what", "find-keys-shard", "unit", Units.READ);

        this.findSeries = new SemanticFutureReporter(registry,
            id.tagged("what", "find-series", "unit", Units.READ));
        this.findSeriesShard = id.tagged("what", "find-keys-shard", "unit", Units.READ);

        this.deleteSeries = new SemanticFutureReporter(registry,
            id.tagged("what", "delete-series", "unit", Units.DELETE));
        this.write =
            new SemanticFutureReporter(registry, id.tagged("what", "write", "unit", Units.WRITE));
        this.search =
            new SemanticFutureReporter(registry, id.tagged("what", "search", "unit", Units.READ));
        this.tagKeySuggest = new SemanticFutureReporter(registry,
            id.tagged("what", "tag-key-suggest", "unit", Units.READ));
        this.tagSuggest = new SemanticFutureReporter(registry,
            id.tagged("what", "tag-suggest", "unit", Units.READ));
        this.count =
            new SemanticFutureReporter(registry, id.tagged("what", "count", "unit", Units.READ));
        this.keySuggest = new SemanticFutureReporter(registry,
            id.tagged("what", "key-suggest", "unit", Units.READ));
        this.tagValuesSuggest = new SemanticFutureReporter(registry,
            id.tagged("what", "tag-values-suggest", "unit", Units.READ));
        this.tagValueSuggest = new SemanticFutureReporter(registry,
            id.tagged("what", "tag-value-suggest", "unit", Units.READ));
    }

    @Override
    public FutureReporter.Context reportFindTags() {
        return findTags.setup();
    }

    @Override
    public FutureReporter.Context reportFindTagsShard(Map<String, String> shard) {
        final Shard s = shards.get(shard);

        if (s == null) {
            throw new IllegalStateException("Unexpected shard: " + shard);
        }

        return s.findTags.setup();
    }

    @Override
    public FutureReporter.Context reportFindKeys() {
        return findKeys.setup();
    }

    @Override
    public FutureReporter.Context reportFindKeysShard(Map<String, String> shard) {
        final Shard s = shards.get(shard);

        if (s == null) {
            throw new IllegalStateException("Unexpected shard: " + shard);
        }

        return s.findKeys.setup();
    }

    @Override
    public FutureReporter.Context reportFindSeries() {
        return findSeries.setup();
    }

    @Override
    public FutureReporter.Context reportFindSeriesShard(Map<String, String> shard) {
        final Shard s = shards.get(shard);

        if (s == null) {
            throw new IllegalStateException("Unexpected shard: " + shard);
        }

        return s.findSeries.setup();
    }

    @Override
    public FutureReporter.Context reportDeleteSeries() {
        return deleteSeries.setup();
    }

    @Override
    public FutureReporter.Context reportWrite() {
        return write.setup();
    }

    @Override
    public FutureReporter.Context reportSearch() {
        return search.setup();
    }

    @Override
    public FutureReporter.Context reportTagKeySuggest() {
        return tagKeySuggest.setup();
    }

    @Override
    public FutureReporter.Context reportTagSuggest() {
        return tagSuggest.setup();
    }

    @Override
    public FutureReporter.Context reportCount() {
        return count.setup();
    }

    @Override
    public FutureReporter.Context reportKeySuggest() {
        return keySuggest.setup();
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
    public void registerShards(Set<Map<String, String>> shards) {
        final Map<Map<String, String>, Shard> newShards = new HashMap<>();

        for (final Map<String, String> shard : shards) {
            final String shardName = formatShard(shard);

            final FutureReporter findTags =
                new SemanticFutureReporter(registry, this.findTagsShard.tagged("shard", shardName));
            final FutureReporter findKeys =
                new SemanticFutureReporter(registry, this.findKeysShard.tagged("shard", shardName));
            final FutureReporter findSeries = new SemanticFutureReporter(registry,
                this.findSeriesShard.tagged("shard", shardName));

            newShards.put(shard, new Shard(findTags, findKeys, findSeries));
        }

        this.shards = newShards;
    }

    private String formatShard(final Map<String, String> shard) {
        if (shard == null) {
            return "null";
        }

        final List<String> entries = new ArrayList<>();

        for (final Map.Entry<String, String> e : shard.entrySet()) {
            entries.add(String.format("%s=%s", e.getKey(), e.getValue()));
        }

        Collections.sort(entries);
        return StringUtils.join(entries, ",");
    }

    @RequiredArgsConstructor
    private static final class Shard {
        private final FutureReporter findTags;
        private final FutureReporter findKeys;
        private final FutureReporter findSeries;
    }
}
