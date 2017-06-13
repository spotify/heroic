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

package com.spotify.heroic.metric;

import static com.google.common.hash.Hashing.murmur3_32;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.spotify.heroic.common.Histogram;
import com.spotify.heroic.common.Series;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public final class ShardedResultGroup {
    // record separator is needed to avoid conflicts
    private static final int RECORD_SEPARATOR = 0;
    private static final HashFunction HASH_FUNCTION = murmur3_32();

    private final Map<String, String> shard;
    private final Map<String, String> key;
    private final Set<Series> series;
    private final MetricCollection metrics;
    private final long cadence;

    public boolean isEmpty() {
        return metrics.isEmpty();
    }

    public int hashGroup() {
        final Hasher hasher = HASH_FUNCTION.newHasher();

        for (Map.Entry<String, String> e : shard.entrySet()) {
            hasher.putInt(RECORD_SEPARATOR);
            hasher.putString(e.getKey(), Charsets.UTF_8);
            hasher.putInt(RECORD_SEPARATOR);
            hasher.putString(e.getValue(), Charsets.UTF_8);
        }

        hasher.putInt(RECORD_SEPARATOR);

        for (Map.Entry<String, String> e : key.entrySet()) {
            hasher.putInt(RECORD_SEPARATOR);
            hasher.putString(e.getKey(), Charsets.UTF_8);
            hasher.putInt(RECORD_SEPARATOR);
            hasher.putString(e.getValue(), Charsets.UTF_8);
        }

        return hasher.hash().asInt();
    }

    public static MultiSummary summarize(List<ShardedResultGroup> resultGroups) {
        final ImmutableSet.Builder<Map<String, String>> shardSummary = ImmutableSet.builder();
        final Histogram.Builder keySize = Histogram.builder();
        final SeriesSetsSummarizer seriesSummarizer = new SeriesSetsSummarizer();
        final Histogram.Builder dataSize = Histogram.builder();
        Optional<Long> cadence = Optional.empty();

        for (ShardedResultGroup rg : resultGroups) {
            shardSummary.add(rg.getShard());
            keySize.add(rg.getKey().size());
            seriesSummarizer.add(rg.getSeries());
            dataSize.add(rg.getMetrics().size());
            cadence = Optional.of(rg.getCadence());
        }

        return new MultiSummary(shardSummary.build(), keySize.build(), seriesSummarizer.end(),
            dataSize.build(), cadence);
    }

    @Data
    public static class MultiSummary {
        private final Set<Map<String, String>> shards;
        private final Histogram keySize;
        private final SeriesSetsSummarizer.Summary series;
        private final Histogram dataSize;
        private final Optional<Long> cadence;
    }
}
