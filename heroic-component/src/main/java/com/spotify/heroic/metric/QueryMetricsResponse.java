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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Series;
import com.spotify.heroic.common.Statistics;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.Data;
import lombok.NonNull;

@Data
@JsonSerialize(using = QueryMetricsResponse.Serializer.class)
public class QueryMetricsResponse {
    @NonNull
    private final UUID queryId;

    @NonNull
    private final DateRange range;

    @NonNull
    private final List<ShardedResultGroup> result;

    @NonNull
    private final Statistics statistics = Statistics.empty();

    @NonNull
    private final List<RequestError> errors;

    @NonNull
    private final QueryTrace trace;

    @NonNull
    private final ResultLimits limits;

    @NonNull
    private final Optional<Long> preAggregationSampleSize;

    @NonNull
    private final Optional<CacheInfo> cache;

    public static class Serializer extends JsonSerializer<QueryMetricsResponse> {
        @Override
        public void serialize(
            QueryMetricsResponse response, JsonGenerator g, SerializerProvider provider
        ) throws IOException {
            final List<ShardedResultGroup> result = response.getResult();
            final Map<String, SortedSet<String>> commonTags =
                calculateCommon(result, Series::getTags);
            final Map<String, SortedSet<String>> commonResource =
                calculateCommon(result, Series::getResource);

            g.writeStartObject();

            g.writeObjectField("queryId", response.getQueryId());
            g.writeObjectField("range", response.getRange());
            g.writeObjectField("trace", response.getTrace());
            g.writeObjectField("limits", response.getLimits());
            g.writeBooleanField("cached",
                response.getCache().map(CacheInfo::isCached).orElse(false));
            g.writeObjectField("cache", response.getCache());

            g.writeFieldName("commonTags");
            serializeCommonTags(g, commonTags);

            g.writeFieldName("commonResource");
            serializeCommonTags(g, commonResource);

            g.writeFieldName("result");
            serializeResult(g, result);

            g.writeObjectField("preAggregationSampleSize", response.getPreAggregationSampleSize());

            g.writeFieldName("errors");
            serializeErrors(g, response.getErrors());

            g.writeEndObject();
        }

        private void serializeCommonTags(
            final JsonGenerator g, final Map<String, SortedSet<String>> common
        ) throws IOException {
            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> e : common.entrySet()) {
                g.writeFieldName(e.getKey());

                g.writeStartArray();

                for (final String value : e.getValue()) {
                    g.writeString(value);
                }

                g.writeEndArray();
            }

            g.writeEndObject();
        }

        private void serializeErrors(final JsonGenerator g, final List<RequestError> errors)
            throws IOException {
            g.writeStartArray();

            for (final RequestError error : errors) {
                g.writeObject(error);
            }

            g.writeEndArray();
        }

        private Map<String, SortedSet<String>> calculateCommon(
            final List<ShardedResultGroup> result,
            final Function<Series, Map<String, String>> accessor
        ) {
            final Iterator<Map<String, SortedSet<String>>> resourcePartitions = result
                .stream()
                .map(r -> partitionMaps(r.getSeries().stream().map(accessor)))
                .iterator();

            if (!resourcePartitions.hasNext()) {
                return ImmutableMap.of();
            }

            final Map<String, SortedSet<String>> common = resourcePartitions.next();

            while (resourcePartitions.hasNext()) {
                final Map<String, SortedSet<String>> next = resourcePartitions.next();
                common.keySet().removeIf(k -> !next.containsKey(k));

                if (common.isEmpty()) {
                    return common;
                }

                for (final Map.Entry<String, SortedSet<String>> e : common.entrySet()) {
                    e.getValue().addAll(next.get(e.getKey()));
                }
            }

            return common;
        }

        private Map<String, SortedSet<String>> partitionMaps(
            final Stream<Map<String, String>> stream
        ) {
            final Map<String, SortedSet<String>> partitioned = new HashMap<>();

            stream.forEach(m -> {
                for (final Map.Entry<String, String> e : m.entrySet()) {
                    partitioned.computeIfAbsent(e.getKey(), k -> new TreeSet<>()).add(e.getValue());
                }
            });

            return partitioned;
        }

        private void serializeResult(
            final JsonGenerator g, final List<ShardedResultGroup> result
        ) throws IOException {

            g.writeStartArray();

            for (final ShardedResultGroup group : result) {
                g.writeStartObject();

                final MetricCollection collection = group.getMetrics();
                final SeriesValues series = SeriesValues.fromSeries(group.getSeries().iterator());

                g.writeStringField("type", collection.getType().identifier());
                g.writeStringField("hash", Integer.toHexString(group.hashGroup()));
                g.writeObjectField("shard", group.getShard());
                g.writeNumberField("cadence", group.getCadence());
                g.writeObjectField("values", collection.data());

                writeKey(g, series.getKeys());

                writeTags(g, series.getTags());
                writeTagCounts(g, series.getTags());

                writeResource(g, series.getResource());
                writeResourceCounts(g, series.getResource());

                g.writeEndObject();
            }

            g.writeEndArray();
        }

        void writeKey(JsonGenerator g, final SortedSet<String> keys) throws IOException {
            g.writeFieldName("key");

            if (keys.size() == 1) {
                g.writeString(keys.iterator().next());
            } else {
                g.writeNull();
            }
        }

        void writeTags(
            JsonGenerator g, final Map<String, SortedSet<String>> tags
        ) throws IOException {
            g.writeFieldName("tags");

            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {
                final SortedSet<String> values = pair.getValue();

                if (values.size() != 1) {
                    continue;
                }

                g.writeStringField(pair.getKey(), values.iterator().next());
            }

            g.writeEndObject();
        }

        void writeTagCounts(JsonGenerator g, final Map<String, SortedSet<String>> tags)
            throws IOException {
            g.writeFieldName("tagCounts");

            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {
                final SortedSet<String> values = pair.getValue();

                if (values.size() <= 1) {
                    continue;
                }

                g.writeNumberField(pair.getKey(), values.size());
            }

            g.writeEndObject();
        }

        void writeResourceCounts(JsonGenerator g, final Map<String, SortedSet<String>> tags)
            throws IOException {
            g.writeFieldName("resourceCounts");

            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> pair : tags.entrySet()) {
                final SortedSet<String> values = pair.getValue();

                if (values.size() <= 1) {
                    continue;
                }

                g.writeNumberField(pair.getKey(), values.size());
            }

            g.writeEndObject();
        }

        void writeResource(
            JsonGenerator g, final Map<String, SortedSet<String>> resource
        ) throws IOException {
            g.writeFieldName("resource");

            g.writeStartObject();

            for (final Map.Entry<String, SortedSet<String>> pair : resource.entrySet()) {
                final SortedSet<String> values = pair.getValue();

                if (values.size() != 1) {
                    continue;
                }

                g.writeStringField(pair.getKey(), values.iterator().next());
            }

            g.writeEndObject();
        }
    }

    public Summary summarize() {
        return new Summary(range, ShardedResultGroup.summarize(result), statistics, errors, trace,
            limits, preAggregationSampleSize, cache);
    }

    // Only include data suitable to log to query log
    @Data
    public class Summary {
        private final DateRange range;
        private final ShardedResultGroup.MultiSummary result;
        private final Statistics statistics;
        private final List<RequestError> errors;
        private final QueryTrace trace;
        private final ResultLimits limits;
        private final Optional<Long> preAggregationSampleSize;
        private final Optional<CacheInfo> cache;
    }
}
