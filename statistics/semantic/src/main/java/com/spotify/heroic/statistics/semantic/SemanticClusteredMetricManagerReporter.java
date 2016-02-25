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

import com.spotify.heroic.cluster.NodeMetadata;
import com.spotify.heroic.statistics.ClusteredMetricManagerReporter;
import com.spotify.heroic.statistics.FutureReporter;
import com.spotify.metrics.core.MetricId;
import com.spotify.metrics.core.SemanticMetricRegistry;
import lombok.ToString;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ToString(of = {})
public class SemanticClusteredMetricManagerReporter implements ClusteredMetricManagerReporter {
    private static final String COMPONENT = "clustered-metric-manager";

    private final SemanticMetricRegistry registry;
    private final FutureReporter query;
    private final FutureReporter write;
    private final Map<Map<String, String>, FutureReporter> shardQueries = new HashMap<>();
    private final MetricId shardQueryBase;

    private static final Comparator<Map.Entry<String, String>> tagsComparator =
        new Comparator<Map.Entry<String, String>>() {
            @Override
            public int compare(Map.Entry<String, String> a, Map.Entry<String, String> b) {
                final int key = a.getKey().compareTo(b.getKey());

                if (key != 0) {
                    return key;
                }

                return a.getValue().compareTo(b.getValue());
            }
        };

    public SemanticClusteredMetricManagerReporter(SemanticMetricRegistry registry) {
        this.registry = registry;
        final MetricId id = MetricId.build().tagged("component", COMPONENT);
        this.query =
            new SemanticFutureReporter(registry, id.tagged("what", "query", "unit", Units.READ));
        this.write =
            new SemanticFutureReporter(registry, id.tagged("what", "write", "unit", Units.WRITE));
        this.shardQueryBase = id.tagged("what", "shard-query", "unit", Units.READ);
    }

    @Override
    public FutureReporter.Context reportQuery() {
        return query.setup();
    }

    @Override
    public FutureReporter.Context reportWrite() {
        return write.setup();
    }

    @Override
    public synchronized FutureReporter.Context reportShardFullQuery(NodeMetadata metadata) {
        final FutureReporter r = shardQueries.get(metadata.getTags());

        if (r != null) {
            return r.setup();
        }

        final FutureReporter newReporter = new SemanticFutureReporter(registry,
            shardQueryBase.tagged("shard", formatShard(metadata.getTags())));
        shardQueries.put(metadata.getTags(), newReporter);
        return newReporter.setup();
    }

    private String formatShard(Map<String, String> tags) {
        if (tags == null) {
            return "<null>";
        }

        final List<Map.Entry<String, String>> entries = new ArrayList<>(tags.entrySet());
        Collections.sort(entries, tagsComparator);
        final List<String> parts = new ArrayList<>(entries.size());

        for (final Map.Entry<String, String> e : entries) {
            parts.add(String.format("%s=%s", e.getKey(), e.getValue()));
        }

        return StringUtils.join(",", parts);
    }
}
