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

package com.spotify.heroic.http.query;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableList;
import com.spotify.heroic.common.DateRange;
import com.spotify.heroic.common.Statistics;
import com.spotify.heroic.metric.MetricCollection;
import com.spotify.heroic.metric.QueryTrace;
import com.spotify.heroic.metric.RequestError;
import com.spotify.heroic.metric.SeriesValues;
import com.spotify.heroic.metric.ShardLatency;
import com.spotify.heroic.metric.ShardedResultGroup;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;

@RequiredArgsConstructor
public class QueryMetricsResponse {
    @Getter
    private final DateRange range;

    @Getter
    @JsonSerialize(using = ResultSerializer.class)
    private final List<ShardedResultGroup> result;

    @Getter
    private final Statistics statistics = Statistics.empty();

    @Getter
    private final List<RequestError> errors;

    /**
     * Shard latencies associated with the query.
     *
     * @deprecated Use {@link #trace} instead.
     */
    @Getter
    private final List<ShardLatency> latencies = ImmutableList.of();

    @Getter
    private final QueryTrace trace;

    public static class ResultSerializer extends JsonSerializer<List<ShardedResultGroup>> {
        @Override
        public void serialize(
            List<ShardedResultGroup> result, JsonGenerator g, SerializerProvider provider
        ) throws IOException, JsonProcessingException {
            g.writeStartArray();

            for (final ShardedResultGroup group : result) {
                g.writeStartObject();

                final MetricCollection collection = group.getGroup();
                final SeriesValues series = group.getSeries();

                g.writeStringField("type", collection.getType().identifier());
                g.writeStringField("hash", Integer.toHexString(group.hashCode()));
                g.writeObjectField("shard", group.getShard());
                g.writeNumberField("cadence", group.getCadence());
                g.writeObjectField("values", collection.getData());
                g.writeNumberField("keyCount", group.getSeries().getKeys().size());

                writeKey(g, series.getKeys());
                writeTags(g, series.getTags());
                writeTagCounts(g, series.getTags());

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

        void writeTags(JsonGenerator g, final Map<String, SortedSet<String>> tags)
            throws IOException {
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
    }
}
